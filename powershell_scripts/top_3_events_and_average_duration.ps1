$BASE = "http://localhost:8000"

Write-Host "`n== Top 3 events by tickets sold =="
Invoke-RestMethod -Method Get -Uri "$BASE/api/v1/analytics/top3-by-tickets" `
  -Headers @{ Accept = "application/json" } |
  ConvertTo-Json -Depth 10

Write-Host "`n== Average event duration by organizer =="
Invoke-RestMethod -Method Get -Uri "$BASE/api/v1/analytics/avg-duration-by-organizer" `
  -Headers @{ Accept = "application/json" } |
  ConvertTo-Json -Depth 10