$BASE = "http://localhost:8000"

$body2 = @{
  vartotojo_id     = "vycka.b@yahoo.com"
  renginys_id      = "EV0012"
  bilieto_tipas    = "B1"
  kiekis           = 15
} | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri "$BASE/api/v1/purchase" -ContentType "application/json" -Body $body2