# Laboratorio Kafka - Nivel 1: Fundamentos

Laboratorio de aprendizaje autoguiado para Apache Kafka 4.0+ usando la arquitectura **KRaft** (sin ZooKeeper) sobre Docker Desktop en Windows 10.

## Objetivos de aprendizaje

Al completar este nivel podrás:
- Levantar un clúster Kafka con KRaft en un solo broker usando Docker
- Ejecutar comandos administrativos básicos (`kafka-topics`, `kafka-broker-api-versions`)
- Producir y consumir tu primer mensaje con código Java

## Requisitos del sistema

| Requisito | Mínimo | Recomendado |
|---|---|---|
| OS | Windows 10 (64-bit) | Windows 10/11 |
| RAM libre | 2 GB | 4 GB |
| Docker Desktop | 4.x+ | Última versión |
| Java | 17 | 17 o 21 |
| Maven | 3.8+ | 3.9+ |
| WSL2 | Recomendado | Recomendado |

## Estructura del proyecto

```
kafka-lab-nivel-1/
├── docker/                     # Infraestructura Docker
│   ├── docker-compose.yml      # Definicion del broker Kafka KRaft
│   └── kafka-config/
│       └── server.properties   # Configuracion del broker
├── scripts/
│   ├── powershell/             # Scripts para PowerShell 5.1/7+
│   │   ├── 00-verificar-requisitos.ps1
│   │   ├── 01-iniciar-kafka.ps1
│   │   ├── 02-verificar-cluster.ps1
│   │   ├── 03-detener-kafka.ps1
│   │   └── 04-limpiar-todo.ps1
│   └── cmd/                    # Scripts equivalentes para CMD
│       ├── 00-verificar-requisitos.bat
│       ├── 01-iniciar-kafka.bat
│       ├── 02-verificar-cluster.bat
│       ├── 03-detener-kafka.bat
│       └── 04-limpiar-todo.bat
├── experimentos/
│   ├── exp-01-primer-mensaje.md  # Guia del primer experimento
│   └── resultados/               # Tus resultados guardados aqui
├── java/
│   ├── pom.xml                   # Dependencias Maven
│   └── src/main/java/com/nexus/kafka/nivel1/
│       ├── SimpleProducer.java   # Producer instrumentado
│       └── SimpleConsumer.java   # Consumer instrumentado
├── logs/                         # Logs locales del laboratorio
├── INSTRUCCIONES-NIVEL-1.md      # Guia paso a paso completa
└── VALIDACION-NIVEL-1.md         # Checklist de validacion
```

## Quick Start

```powershell
# 1. Verificar que el sistema esta listo
.\scripts\powershell\00-verificar-requisitos.ps1

# 2. Iniciar Kafka
.\scripts\powershell\01-iniciar-kafka.ps1

# 3. Verificar el cluster
.\scripts\powershell\02-verificar-cluster.ps1

# 4. Compilar el codigo Java
cd java
mvn clean package -q
cd ..

# 5. Seguir el experimento
# Abre INSTRUCCIONES-NIVEL-1.md para la guia completa
```

## Troubleshooting en Windows

### Docker Desktop no inicia
- Verifica que WSL2 está habilitado: `wsl --status`
- Reinicia Docker Desktop desde la bandeja del sistema
- Revisa los logs en `%APPDATA%\Docker\log\`

### Puerto 9092 ocupado
```powershell
# Ver qué proceso usa el puerto
Get-NetTCPConnection -LocalPort 9092 | Select-Object OwningProcess
Get-Process -Id <PID_del_resultado>
```

### El contenedor no inicia
```powershell
# Ver logs detallados del contenedor
docker logs kafka-nivel1 --tail 50
```

### Error "no se puede conectar" desde Java
- Verifica que el contenedor está corriendo: `docker ps`
- Prueba la conexión: `Test-NetConnection -ComputerName localhost -Port 9092`
- Asegúrate de estar usando `localhost:9092` (no `kafka:9092` desde Windows)

### Scripts PowerShell bloqueados por política de ejecución
```powershell
# Ejecutar en PowerShell como Administrador:
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

## Recursos de referencia

- [Documentación oficial de Apache Kafka](https://kafka.apache.org/documentation/)
- [KRaft: Kafka sin ZooKeeper (KIP-500)](https://cwiki.apache.org/confluence/display/KAFKA/KIP-500%3A+Replace+ZooKeeper+with+a+Self-Managed+Metadata+Quorum)
- [Docker Hub: apache/kafka](https://hub.docker.com/r/apache/kafka)
- [Kafka clients Java API](https://kafka.apache.org/documentation/#api)
