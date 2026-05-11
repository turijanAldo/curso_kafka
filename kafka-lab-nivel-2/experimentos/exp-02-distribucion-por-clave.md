# Experimento 02 - Distribución de Mensajes por Clave

## Objetivo

Demostrar que mensajes con la misma clave **siempre** van a la misma partición, y que diferentes claves pueden ir a particiones diferentes.

## Hipótesis

Si envío 20 mensajes con clave `user-123`, todos llegarán a la misma partición. Si envío 20 mensajes con clave `user-456`, pueden ir a una partición diferente. El número de partición está determinado por `hash(clave) % num_particiones`.

## Pre-requisitos

- [ ] Clúster Kafka iniciado (`01-iniciar-kafka.ps1` o `.bat`)
- [ ] Topic `transacciones-4p` creado (`10-crear-topics-particionados.ps1`)
- [ ] Java compilado: `cd java && mvn clean package`
- [ ] `JAVA_HOME` configurado correctamente

## Procedimiento

### Paso 1 — Verificar que el topic tiene 4 particiones

```powershell
docker exec kafka-nivel1 /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 `
    --describe `
    --topic transacciones-4p
```

**Esperado:** Ver `PartitionCount: 4`

---

### Paso 2 — Enviar 20 mensajes con clave `user-123`

Desde la carpeta `kafka-lab-nivel-2\java\`:

```powershell
for ($i = 1; $i -le 20; $i++) {
    & "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-2-1.0.0.jar `
        com.nexus.kafka.nivel2.KeyedProducer `
        transacciones-4p user-123 "{`"monto`": $($i * 10)}"
    Start-Sleep -Milliseconds 200
}
```

**CMD equivalente:**
```cmd
for /L %i in (1,1,20) do (
    "%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-2-1.0.0.jar com.nexus.kafka.nivel2.KeyedProducer transacciones-4p user-123 "{\"monto\": %i0}"
    timeout /t 1 /nobreak >nul
)
```

**Observa:** Todos los mensajes deben mostrar el mismo número de partición.

---

### Paso 3 — Enviar 20 mensajes con clave `user-456`

```powershell
for ($i = 1; $i -le 20; $i++) {
    & "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-2-1.0.0.jar `
        com.nexus.kafka.nivel2.KeyedProducer `
        transacciones-4p user-456 "{`"monto`": $($i * 5)}"
    Start-Sleep -Milliseconds 200
}
```

**Observa:** ¿Van a la misma partición que `user-123` o a una diferente?

---

### Paso 4 — Enviar 20 mensajes con clave `user-789`

```powershell
for ($i = 1; $i -le 20; $i++) {
    & "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-2-1.0.0.jar `
        com.nexus.kafka.nivel2.KeyedProducer `
        transacciones-4p user-789 "{`"monto`": $($i * 3)}"
    Start-Sleep -Milliseconds 200
}
```

---

### Paso 5 — Analizar la distribución con PartitionAnalyzer

```powershell
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-2-1.0.0.jar `
    com.nexus.kafka.nivel2.PartitionAnalyzer transacciones-4p
```

**CMD:**
```cmd
"%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-2-1.0.0.jar com.nexus.kafka.nivel2.PartitionAnalyzer transacciones-4p
```

---

## Resultados esperados

### Output del KeyedProducer (todos los mensajes `user-123` deben coincidir):

```
🔑 Clave del mensaje  : user-123
📊 Hash murmur2       : -1789724228
📐 Particion teorica  : -1789724228 % 4 = X
📤 Enviando mensaje...
✅ Mensaje enviado:
   Topic     : transacciones-4p
   Partition : X ✓ (coincide con calculo teorico)
   Offset    : 0..19
   Key       : user-123
```

### Output del PartitionAnalyzer:

```
+-----------+-------------+--------------+----------+
| Partition | First Offset| Last Offset  | Mensajes |
+==========================================================+
|     0     |      0      |     20       |    20    |   <- user-123 (hipotesis)
|     1     |      0      |      0       |     0    |
|     2     |      0      |     20       |    20    |   <- user-456 (hipotesis)
|     3     |      0      |     20       |    20    |   <- user-789 (hipotesis)
+-----------+-------------+--------------+----------+
Total de mensajes: 60
```

*(Los números reales de partición dependen del hash de cada clave con murmur2)*

---

## Análisis

### ¿Por qué la misma clave siempre va a la misma partición?

Kafka usa el algoritmo **murmur2** para calcular el hash de la clave serializada. El algoritmo es determinista: la misma entrada siempre produce el mismo hash. La fórmula es:

```
particion = toPositive(murmur2(keyBytes)) % numParticiones
```

Donde `toPositive` garantiza que el resultado es positivo (el hash puede ser negativo en Java por overflow de int).

### ¿Por qué esto es crítico en sistemas reales?

Imagina un sistema de pagos donde procesas eventos de un usuario: `cuenta_creada`, `deposito`, `retiro`, `cierre`. Si estos eventos se procesan en diferente orden, el sistema puede quedar en estado inconsistente.

Con claves:
- `user-123` siempre va a la partición 2
- La partición 2 mantiene el **orden de inserción** de todos sus mensajes
- El consumer que lee la partición 2 procesa los eventos de `user-123` **en orden**

Sin claves (round-robin):
- Los eventos del mismo usuario pueden ir a diferentes particiones
- Pueden ser procesados en diferente orden por diferentes consumers
- ❌ El orden no está garantizado entre particiones

---

## Conclusiones

- [ ] Confirmé que la misma clave produce siempre la misma partición
- [ ] Entiendo el algoritmo `hash(key) % particiones`
- [ ] Entiendo por qué las claves garantizan orden por entidad
- [ ] Vi que diferentes claves pueden ir a la misma o diferente partición

## Espacio para resultados reales

### Partición asignada a `user-123`: ___
### Partición asignada a `user-456`: ___
### Partición asignada a `user-789`: ___

### Output del PartitionAnalyzer:
```
(pegar aquí)
```
