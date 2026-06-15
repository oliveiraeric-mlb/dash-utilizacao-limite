# update.ps1 — Rebuild dashboard e publica no GitHub
# Executado automaticamente pelo Windows Task Scheduler

$ErrorActionPreference = "Stop"
$repo = "C:\Users\ericolivei\Desktop\dash-utilizacao"
$log  = "$repo\update.log"

function Log($msg) {
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    "$ts  $msg" | Tee-Object -FilePath $log -Append
}

Log "=== Iniciando rebuild ==="

try {
    Set-Location $repo

    # 1. Rebuild HTML com dados frescos do BQ
    Log "Rodando rebuild_dash.py..."
    $result = python3 rebuild_dash.py 2>&1
    Log $result

    # 2. Commit e push se houve mudança
    $diff = git diff --name-only docs/index.html
    if ($diff) {
        $date = Get-Date -Format "yyyy-MM-dd"
        git add docs/index.html
        git commit -m "chore: rebuild $date"
        git push
        Log "Push concluido para GitHub Pages"
    } else {
        Log "Sem mudancas no HTML, push ignorado"
    }

    Log "=== Concluido com sucesso ==="
} catch {
    Log "ERRO: $_"
    exit 1
}
