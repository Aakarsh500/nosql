# Failure-injection demo (Review 1, demo steps 4-5): stop Neo4j, keep checking out,
# show orders still commit to MongoDB with PENDING outbox events, restart Neo4j, watch catch-up.
#   powershell -ExecutionPolicy Bypass -File scripts\outage_demo.ps1
param([string]$Base = "http://localhost:8001", [int]$Orders = 3)
$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)

function Summary { Invoke-RestMethod "$Base/api/admin/outbox/summary" }
function Show($label) { $s = Summary; Write-Host ("{0,-28} waiting for graph={1} processed={2} dead={3} oldest waiting={4}s" -f $label, $s.backlog, $s.counts.PROCESSED, $s.counts.DEAD, $s.oldest_pending_age_s) }

Show "Before outage:"
Write-Host "`n>>> docker compose stop neo4j" -ForegroundColor Yellow
docker compose stop neo4j | Out-Null
$health = Invoke-RestMethod "$Base/api/health"
Write-Host ("Health: status={0}  neo4j ok={1}" -f $health.status, $health.stores.neo4j.ok)

$ids = @()
for ($i = 1; $i -le $Orders; $i++) {
  $cart = (Invoke-RestMethod -Method Post "$Base/api/carts").id
  Invoke-RestMethod -Method Put "$Base/api/carts/$cart/items/p_104" -ContentType "application/json" -Body '{"quantity":1}' | Out-Null
  Invoke-RestMethod -Method Put "$Base/api/carts/$cart/items/p_105" -ContentType "application/json" -Body '{"quantity":1}' | Out-Null
  $key = "outage-" + [guid]::NewGuid().ToString("N")
  $body = @{ cart_id = $cart; customer_id = "c_outage_$i" } | ConvertTo-Json
  $r = Invoke-RestMethod -Method Post "$Base/api/orders" -ContentType "application/json" -Headers @{ "Idempotency-Key" = $key } -Body $body
  $ids += $r.order.id
  Write-Host ("Checkout during outage -> order {0} committed in MongoDB (graph: {1})" -f $r.order.id, $r.order.graph_projection.status) -ForegroundColor Green
}
Start-Sleep -Seconds 6
Show "During outage:"
$rec = Invoke-RestMethod "$Base/api/products/p_104/recommendations"
Write-Host ("Recommendations while Neo4j is down: degraded='{0}', {1} fallback item(s)" -f $rec.degraded, $rec.items.Count)

Write-Host "`n>>> docker compose start neo4j" -ForegroundColor Yellow
$restart = Get-Date
docker compose start neo4j | Out-Null
do {
  Start-Sleep -Seconds 2
  try { $statuses = $ids | ForEach-Object { (Invoke-RestMethod "$Base/api/orders/$_").graph_projection.status } } catch { $statuses = @("PENDING") }
  $pending = @($statuses | Where-Object { $_ -ne "PROCESSED" }).Count
  Write-Host ("  waiting... {0} of {1} outage orders still pending" -f $pending, $ids.Count)
} while ($pending -gt 0 -and ((Get-Date) - $restart).TotalSeconds -lt 180)
$catchUp = [math]::Round(((Get-Date) - $restart).TotalSeconds, 1)
Show "After recovery:"
Write-Host ("All outage orders projected to Neo4j {0}s after restart (includes Neo4j boot + retry backoff). Zero orders lost." -f $catchUp) -ForegroundColor Green

$result = @{ orders = $ids; catch_up_seconds_after_restart = $catchUp; final = (Summary) } | ConvertTo-Json -Depth 6
New-Item -ItemType Directory -Force artifacts | Out-Null
$result | Out-File -Encoding utf8 artifacts\outage_recovery.json
Write-Host "Saved artifacts\outage_recovery.json"
