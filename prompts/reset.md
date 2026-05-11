# INSTRUCTIVO: Reset Completo del Laboratorio Kafka

## Escenarios de Reset

### Reset Suave (mantener configuración, limpiar datos)

**PowerShell:**
```powershell
# Detener clúster
cd kafka-lab-nivel-X\docker
docker-compose down

# Eliminar volúmenes (borra todos los mensajes y topics)
docker volume rm $(docker volume ls -q | Select-String "kafka")

# Limpiar logs locales
cd ..\logs
Remove-Item * -Recurse -Force
New-Item .gitkeep -ItemType File

# Reiniciar
cd ..\docker
docker-compose up -d
```

**CMD:**
```bat
cd kafka-lab-nivel-X\docker
docker-compose down
docker volume ls -q | findstr kafka > volumes.txt
for /f %i in (volumes.txt) do docker volume rm %i
cd ..\logs
del /s /q *
echo. > .gitkeep
cd ..\docker
docker-compose up -d
```

### Reset Completo (empezar desde cero)

**PowerShell:**
```powershell
# Detener y eliminar TODO
cd kafka-lab-nivel-X\docker
docker-compose down -v

# Eliminar imágenes Kafka (opcional, ahorra espacio)
docker images | Select-String "kafka" | ForEach-Object {
    $imageId = ($_ -split '\s+')[2]
    docker rmi $imageId
}

# Limpiar directorios de resultados
cd ..\experimentos\resultados
Remove-Item *.txt -Force

cd ..\..\logs
Remove-Item * -Recurse -Force
New-Item .gitkeep -ItemType File

# Reiniciar
cd ..\docker
docker-compose up -d
```

**CMD:**
```bat
cd kafka-lab-nivel-X\docker
docker-compose down -v
docker images | findstr kafka > kafka-images.txt
for /f "tokens=3" %i in (kafka-images.txt) do docker rmi %i
cd ..\experimentos\resultados
del /q *.txt
cd ..\..\logs
del /s /q *
echo. > .gitkeep
cd ..\docker
docker-compose up -d
```

### Reset de Topics Específicos (mantener clúster corriendo)

**PowerShell:**
```powershell
# Listar topics
docker exec kafka-broker-1 kafka-topics --list --bootstrap-server localhost:9092

# Eliminar topic específico
docker exec kafka-broker-1 kafka-topics --delete `
  --topic transacciones-4p `
  --bootstrap-server localhost:9092

# Eliminar todos los topics de experimentos
$topics = docker exec kafka-broker-1 kafka-topics --list --bootstrap-server localhost:9092
foreach ($topic in $topics) {
    if ($topic -match "transacciones-") {
        docker exec kafka-broker-1 kafka-topics --delete `
          --topic $topic `
          --bootstrap-server localhost:9092
    }
}
```

**CMD:**
```bat
REM Listar topics
docker exec kafka-broker-1 kafka-topics --list --bootstrap-server localhost:9092

REM Eliminar topic específico
docker exec kafka-broker-1 kafka-topics --delete --topic transacciones-4p --bootstrap-server localhost:9092

REM Para eliminar varios, ejecutar comando anterior para cada topic
```

### Reset de Consumer Groups

**PowerShell:**
```powershell
# Listar consumer groups
docker exec kafka-broker-1 kafka-consumer-groups --list --bootstrap-server localhost:9092

# Eliminar consumer group específico
docker exec kafka-broker-1 kafka-consumer-groups --delete `
  --group grupo-nivel-2 `
  --bootstrap-server localhost:9092

# Resetear offsets de un group (volver a leer desde el inicio)
docker exec kafka-broker-1 kafka-consumer-groups --reset-offsets `
  --group grupo-nivel-2 `
  --topic transacciones-4p `
  --to-earliest `
  --execute `
  --bootstrap-server localhost:9092
```

### Script Automatizado de Reset

**Archivo: `scripts/powershell/99-reset-laboratorio.ps1`**

```powershell
param(
    [Parameter(Mandatory=$false)]
    [ValidateSet("suave", "completo", "topics", "groups")]
    [string]$Tipo = "suave"
)

Write-Host "🔄 Reset del Laboratorio Kafka - Tipo: $Tipo" -ForegroundColor Yellow
Write-Host ""

switch ($Tipo) {
    "suave" {
        Write-Host "⚠️  Esto eliminará todos los mensajes pero mantendrá la configuración" -ForegroundColor Yellow
        $confirm = Read-Host "¿Continuar? (S/N)"
        if ($confirm -ne "S") { exit }
        
        docker-compose down
        docker volume ls -q | Where-Object { $_ -match "kafka" } | ForEach-Object { docker volume rm $_ }
        Remove-Item ..\logs\* -Recurse -Force -ErrorAction SilentlyContinue
        New-Item ..\logs\.gitkeep -ItemType File -Force
        docker-compose up -d
        
        Write-Host "✅ Reset suave completado" -ForegroundColor Green
    }
    
    "completo" {
        Write-Host "⚠️  Esto eliminará TODO incluyendo imágenes Docker" -ForegroundColor Red
        $confirm = Read-Host "¿Continuar? (S/N)"
        if ($confirm -ne "S") { exit }
        
        docker-compose down -v
        docker images | Select-String "kafka" | ForEach-Object {
            $imageId = ($_ -split '\s+')[2]
            docker rmi $imageId -f
        }
        Remove-Item ..\experimentos\resultados\*.txt -Force -ErrorAction SilentlyContinue
        Remove-Item ..\logs\* -Recurse -Force -ErrorAction SilentlyContinue
        New-Item ..\logs\.gitkeep -ItemType File -Force
        docker-compose up -d
        
        Write-Host "✅ Reset completo finalizado" -ForegroundColor Green
    }
    
    "topics" {
        $topics = docker exec kafka-broker-1 kafka-topics --list --bootstrap-server localhost:9092
        foreach ($topic in $topics) {
            docker exec kafka-broker-1 kafka-topics --delete --topic $topic --bootstrap-server localhost:9092
        }
        Write-Host "✅ Topics eliminados" -ForegroundColor Green
    }
    
    "groups" {
        $groups = docker exec kafka-broker-1 kafka-consumer-groups --list --bootstrap-server localhost:9092
        foreach ($group in $groups) {
            docker exec kafka-broker-1 kafka-consumer-groups --delete --group $group --bootstrap-server localhost:9092
        }
        Write-Host "✅ Consumer groups eliminados" -ForegroundColor Green
    }
}

Write-Host ""
Write-Host "📊 Estado del clúster:" -ForegroundColor Cyan
docker ps | Select-String "kafka"
```

**Uso:**
```powershell
# Reset suave (por defecto)
.\scripts\powershell\99-reset-laboratorio.ps1

# Reset completo
.\scripts\powershell\99-reset-laboratorio.ps1 -Tipo completo

# Solo eliminar topics
.\scripts\powershell\99-reset-laboratorio.ps1 -Tipo topics

# Solo eliminar consumer groups
.\scripts\powershell\99-reset-laboratorio.ps1 -Tipo groups
```

---

## Verificación Post-Reset

Después de cualquier reset, verificar:

```powershell
# Clúster corriendo
docker ps

# No hay topics
docker exec kafka-broker-1 kafka-topics --list --bootstrap-server localhost:9092

# No hay consumer groups
docker exec kafka-broker-1 kafka-consumer-groups --list --bootstrap-server localhost:9092

# Broker funcional
docker logs kafka-broker-1 --tail 50
```

---

## Troubleshooting

**Error: "Cannot remove volume in use"**
```powershell
docker-compose down
docker ps -a | Select-String "kafka" | ForEach-Object { docker rm -f ($_ -split '\s+')[0] }
docker volume prune -f
```

**Error: "Port already in use"**
```powershell
# Encontrar proceso usando puerto 9092
netstat -ano | findstr :9092
# Matar proceso (usar PID de la columna final)
taskkill /PID <PID> /F
```

**Clúster no inicia después de reset**
```powershell
# Verificar logs
docker logs kafka-broker-1

# Recrear desde cero
docker-compose down -v
docker system prune -a
docker-compose up -d
```