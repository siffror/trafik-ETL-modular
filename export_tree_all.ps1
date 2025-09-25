# export_tree_py.ps1
param(
  [string]$Root = ".",
  # Mappar som ska exkluderas helt
  [string[]]$ExcludeDirs = @("__pycache__", ".mypy_cache", ".git", ".venv", "logs", "data"),
  # Filmönster att exkludera (utöver att vi ändå bara tar *.py)
  [string[]]$ExcludeFiles = @()
)

function Show-Tree {
  param([string]$Path, [string]$Prefix = "")

  # Mappar (exkludera listan ovan)
  $dirs = Get-ChildItem -LiteralPath $Path -Directory -ErrorAction SilentlyContinue |
          Where-Object { $ExcludeDirs -notcontains $_.Name } |
          Sort-Object Name

  # Endast .py-filer (filtrera på mönster och ev. extra exkludering)
  $files = Get-ChildItem -LiteralPath $Path -File -Filter *.py -ErrorAction SilentlyContinue |
           Where-Object {
             $name = $_.Name
             -not ($ExcludeFiles | ForEach-Object { $name -like $_ } | Where-Object { $_ }) # inga träffar
           } |
           Sort-Object Name

  # Skriv mappar först
  for ($i = 0; $i -lt $dirs.Count; $i++) {
    $d = $dirs[$i]
    $isLast = ($i -eq $dirs.Count - 1 -and $files.Count -eq 0)
    $marker = if ($isLast) { "`-- " } else { "+-- " }
    $nextPrefix = if ($isLast) { "$Prefix    " } else { "$Prefix|   " }
    Write-Output ("{0}{1}{2}" -f $Prefix, $marker, $d.Name)
    Show-Tree -Path $d.FullName -Prefix $nextPrefix
  }

  # Skriv .py-filer
  for ($j = 0; $j -lt $files.Count; $j++) {
    $f = $files[$j]
    $isLastFile = ($j -eq $files.Count - 1)
    $marker = if ($isLastFile) { "`-- " } else { "+-- " }
    Write-Output ("{0}{1}{2}" -f $Prefix, $marker, $f.Name)
  }
}

$rootPath = (Resolve-Path $Root).Path
Write-Output "=== Tree (.py only) from: $rootPath ==="
Write-Output (Split-Path -Leaf $rootPath)
Show-Tree -Path $rootPath
