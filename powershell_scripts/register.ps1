$BASE = "http://localhost:8000"

Write-Host "== Register user =="
$body = @{
  vartotojo_id = "kas.klause14@gmail.com"
  Vardas       = "Kas"
  Pavarde      = "Klause"
  Gimimo_data  = "2003-04-01T00:00:00Z"
  Tel_numeris  = "869260605"
  Miestas      = "Vilnius"
  Pomegiai     = @("Koncertai","Teatras","Komedija")
} | ConvertTo-Json -Depth 5

Invoke-RestMethod -Method Post -Uri "$BASE/api/v1/register" `
  -ContentType "application/json" -Body $body |
  ConvertTo-Json -Depth 10

Write-Host "`n== All events in Vilnius =="
Invoke-RestMethod -Method Get -Uri "$BASE/api/v1/analytics/vilnius-events" `
  -Headers @{ Accept = "application/json" } |
  ConvertTo-Json -Depth 10