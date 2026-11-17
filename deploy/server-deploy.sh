#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Runs ON the EC2 server. .github/workflows/deploy.yml pipes it over SSH:
#
#   ssh server 'bash -s -- <commit> <pm2-name> <repo-match> <port> [folder]' < deploy/server-deploy.sh
#
# Production (seen Nov 17 2026): Amazon Linux 2023, login ec2-user, node/npm/
# pm2 in /usr/bin; ~/Aksharum-school-backend (pm2 "school-backend", cluster,
# :3010) and ~/webscoket (pm2 "ws-gateway", fork, :3020).
#
#   1. finds the server's copy of this repo: [folder] if given, else the folder
#      the pm2 process runs from, else a git checkout whose origin URL
#      contains <repo-match>. Never touches a checkout of another repo.
#   2. moves it to <commit>. Edits to tracked files made on the server are
#      stashed (git stash list), never thrown away; untracked files — .env,
#      uploads/, logs/ — are not touched.
#   3. runs `npm ci --omit=dev` only when package.json / package-lock.json
#      changed, node_modules is missing, or `npm ls` finds it out of step.
#   4. pm2 startOrReload ecosystem.config.js — cluster apps reload worker by
#      worker, so the API stays up.
#   5. checks the app answers http://127.0.0.1:<port>/health and is not
#      crash-looping; if it is, puts the previous commit back and fails.
#
# The same file is used by school-backend and websocket-gateway.
# Lines starting ::error:: / ::warning:: become annotations on the GitHub run.
# ─────────────────────────────────────────────────────────────────────────────

# Everything is inside main() so bash has read the whole script before any
# command runs — otherwise a command that reads stdin could swallow the rest
# of the script coming down the SSH pipe.
main() {
    set -Eeuo pipefail

    local SHA="${1:?commit to deploy}"
    local APP_NAME="${2:?pm2 app name}"
    local REPO_MATCH="${3:?text the git origin URL must contain}"
    PORT="${4:?port that answers /health}"
    local APP_DIR="${5:-}"
    local SCRIPT=server.js

    # ── node / npm / pm2 on PATH ────────────────────────────────────────────
    # A non-interactive SSH login skips ~/.bashrc, which is where nvm adds them.
    export NVM_DIR="${NVM_DIR:-$HOME/.nvm}"
    if [ -s "$NVM_DIR/nvm.sh" ]; then
        set +u; . "$NVM_DIR/nvm.sh" >/dev/null 2>&1 || true; set -u
    fi
    if ! command -v pm2 >/dev/null 2>&1; then
        local d
        for d in $(ls -d "$NVM_DIR"/versions/node/*/bin 2>/dev/null | sort -rV || true) "$HOME/.local/bin" /usr/local/bin; do
            if [ -x "$d/pm2" ]; then export PATH="$d:$PATH"; break; fi
        done
    fi
    command -v node >/dev/null 2>&1 || fail "node is not on PATH for a non-interactive SSH login as $(whoami)"
    command -v pm2  >/dev/null 2>&1 || fail "pm2 is not on PATH for a non-interactive SSH login as $(whoami)"
    pm2 ping >/dev/null 2>&1 || true   # wakes the pm2 daemon so its notices are out of the way
    echo "node $(node -v) · npm $(npm -v) · pm2 $(pm2 -v 2>/dev/null | tail -1) · user $(whoami)"

    # ── which folder ────────────────────────────────────────────────────────
    [ -n "$APP_DIR" ] || APP_DIR="$(pm2q cwd "$APP_NAME")"
    if [ -z "$APP_DIR" ]; then
        local c
        for c in "$HOME"/*/ "$HOME"/*/*/ /var/www/*/ /opt/*/ /srv/*/; do
            [ -d "${c}.git" ] || continue
            if git -C "$c" remote get-url origin 2>/dev/null | grep -qiF -- "$REPO_MATCH"; then APP_DIR="${c%/}"; break; fi
        done
    fi
    [ -n "$APP_DIR" ] || fail "Could not find $APP_NAME on the server: $(whoami)'s pm2 has no process with that name and there is no git checkout of *$REPO_MATCH* under $HOME, /var/www, /opt or /srv. Set the repository variable DEPLOY_DIR to its folder."
    APP_DIR="${APP_DIR%/}"
    [ -d "$APP_DIR/.git" ] || fail "$APP_DIR is not a git checkout — clone the repository there so the deploy can update it"
    local ORIGIN
    ORIGIN="$(git -C "$APP_DIR" remote get-url origin 2>/dev/null || true)"
    case "$(printf %s "$ORIGIN" | tr '[:upper:]' '[:lower:]')" in
        *"$(printf %s "$REPO_MATCH" | tr '[:upper:]' '[:lower:]')"*) ;;
        *) fail "$APP_DIR is a checkout of '$ORIGIN', not of $REPO_MATCH — refusing to touch it" ;;
    esac
    echo "folder $APP_DIR"

    # ── what runs now ───────────────────────────────────────────────────────
    # A pm2 process started from this folder under another name (an older name
    # such as school-app) would keep serving the old code beside the new one.
    local OTHERS TOTAL ONLINE RESTARTS
    OTHERS="$(pm2q others "$APP_NAME" "$APP_DIR" "$SCRIPT")"
    read -r TOTAL ONLINE RESTARTS <<<"$(pm2q state "$APP_NAME")"
    if [ "$TOTAL" = 0 ] && [ -z "$OTHERS" ] && curl -fsS -m 3 -o /dev/null "http://127.0.0.1:$PORT/health" 2>/dev/null; then
        fail "Port $PORT already answers, but $(whoami)'s pm2 has no $APP_NAME process. It may run under root (sudo pm2) or outside pm2 — stop that copy so the deploy can manage the app, or deploy as the user that runs it."
    fi

    # ── move the code ───────────────────────────────────────────────────────
    cd "$APP_DIR"
    OLD="$(git rev-parse HEAD)"
    step "Fetching ${SHA:0:7}"
    git fetch --quiet origin || fail "git fetch failed in $APP_DIR (origin $ORIGIN)"
    git cat-file -e "${SHA}^{commit}" 2>/dev/null || fail "Commit $SHA is not in $APP_DIR after git fetch"

    if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
        echo "::warning::$APP_DIR had edits to tracked files made on the server. They were kept in git stash (see: git stash list) before deploying:"
        git status --short --untracked-files=no
        git -c user.name=deploy -c user.email=deploy@localhost stash push --quiet \
            -m "server edits before deploying ${SHA:0:7} ($(date -u +%FT%TZ))"
    fi

    DEPLOY_SHA="$SHA"
    MOVED=1
    git reset --quiet --hard "$SHA"
    echo "${OLD:0:7} → ${SHA:0:7}"
    git --no-pager log --oneline --no-decorate "$OLD..$SHA" 2>/dev/null | head -20 || true

    if deps_changed "$OLD" "$SHA"; then install_deps; else echo "Dependencies unchanged — npm ci skipped"; fi
    mkdir -p logs

    # ── restart ─────────────────────────────────────────────────────────────
    step "Restarting $APP_NAME"
    local n
    for n in $OTHERS; do
        echo "::warning::Removing pm2 process '$n' — it ran $SCRIPT from $APP_DIR under an older name; ecosystem.config.js runs it as '$APP_NAME'"
        pm2 delete "$n" >/dev/null
    done
    pm2 startOrReload ecosystem.config.js --update-env

    # ── is it up ────────────────────────────────────────────────────────────
    step "Checking $APP_NAME stays up"
    healthy "$PORT" || rollback "$APP_NAME did not answer http://127.0.0.1:$PORT/health after the restart"
    read -r TOTAL ONLINE RESTARTS <<<"$(pm2q state "$APP_NAME")"
    sleep 20
    local TOTAL2 ONLINE2 RESTARTS2
    read -r TOTAL2 ONLINE2 RESTARTS2 <<<"$(pm2q state "$APP_NAME")"
    { [ "$TOTAL2" -gt 0 ] && [ "$ONLINE2" = "$TOTAL2" ]; } || rollback "$APP_NAME: only $ONLINE2 of $TOTAL2 pm2 instances are online after the restart"
    [ "$RESTARTS2" = "$RESTARTS" ] || rollback "$APP_NAME keeps restarting (pm2 restart count $RESTARTS → $RESTARTS2) — it is crashing; see pm2 logs $APP_NAME"
    healthy "$PORT" || rollback "$APP_NAME stopped answering /health"

    MOVED=0
    pm2 save >/dev/null
    echo "✅ $APP_NAME runs ${SHA:0:7} ($TOTAL2 instance(s) online)"
}

OLD=""
DEPLOY_SHA=""
PORT=""
MOVED=0

fail() { echo "::error::$*"; exit 1; }
step() { printf '\n── %s\n' "$*"; }

# pm2q cwd <name>                  → folder the named process runs from
# pm2q state <name>                → "<instances> <online> <total restarts>"
# pm2q others <name> <dir> <script> → other process names running <script> from <dir>
pm2q() {
    pm2 jlist 2>/dev/null | node -e '
        // The JSON is one line; pm2 may print coloured notices around it.
        let list = null;
        for (const line of require("fs").readFileSync(0, "utf8").split("\n").reverse()) {
            if (!line.trim().startsWith("[")) continue;
            try { const v = JSON.parse(line); if (Array.isArray(v)) { list = v; break; } } catch (e) {}
        }
        if (!list) { console.error("::error::pm2 jlist did not answer with a process list"); process.exit(1); }
        const [what, name, dir, script] = process.argv.slice(1);
        const env = (p) => p.pm2_env || {};
        const mine = list.filter((p) => p.name === name);
        if (what === "cwd") console.log(env(mine[0] || {}).pm_cwd || "");
        if (what === "state") console.log(mine.length, mine.filter((p) => env(p).status === "online").length,
            mine.reduce((n, p) => n + (env(p).restart_time || 0), 0));
        if (what === "others") console.log([...new Set(list
            .filter((p) => p.name !== name
                && String(env(p).pm_cwd || "").replace(/\/+$/, "") === dir
                && String(env(p).pm_exec_path || "").split("/").pop() === script)
            .map((p) => p.name))].join("\n"));
    ' "$@"
}

# Reinstall when node_modules is missing, the package files differ between
# the two commits, or what is installed no longer matches package.json
# (npm ls) — e.g. after someone pulled by hand without installing.
deps_changed() {
    [ -d node_modules ] || return 0
    git diff --quiet "$1" "$2" -- package.json package-lock.json || return 0
    npm ls --omit=dev --depth=0 >/dev/null 2>&1 || return 0
    return 1
}

install_deps() {
    step "Installing dependencies (npm ci --omit=dev)"
    npm ci --omit=dev --no-audit --no-fund
}

healthy() {
    local i
    for i in $(seq 1 30); do
        curl -fsS -m 5 -o /dev/null "http://127.0.0.1:$1/health" 2>/dev/null && return 0
        sleep 3
    done
    return 1
}

# Puts the commit that ran before this deploy back, and fails the run.
rollback() {
    MOVED=0
    set +e
    echo "::error::$1"
    if [ -z "$OLD" ] || [ "$OLD" = "$DEPLOY_SHA" ]; then exit 1; fi
    step "Rolling back to ${OLD:0:7}"
    git reset --quiet --hard "$OLD"
    if deps_changed "$DEPLOY_SHA" "$OLD"; then install_deps; fi
    pm2 startOrReload ecosystem.config.js --update-env
    pm2 save >/dev/null
    if healthy "$PORT"; then
        echo "::warning::Rolled back — ${OLD:0:7} is running again. ${DEPLOY_SHA:0:7} was NOT deployed."
    else
        echo "::error::The previous version ${OLD:0:7} did not come back either — check on the server: pm2 logs"
    fi
    exit 1
}

# Any failed command after the code moved puts the previous commit back.
on_error() {
    if [ "$MOVED" = 1 ]; then rollback "Deploy step failed (line $1: $2)"; fi
    echo "::error::Deploy failed (line $1: $2)"
}
trap 'on_error "$LINENO" "$BASH_COMMAND"' ERR

main "$@"
