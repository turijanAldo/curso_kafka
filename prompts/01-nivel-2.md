# LABORATORIO KAFKA - NIVEL 2: Particiones en Acción
## Contexto del proyecto

Este es el Nivel Dos del laboratorio Kafka, enfocado en entender profundamente cómo funcionan las particiones. Construye sobre el Nivel Uno donde ya tienes un broker funcional.

**Objetivo de aprendizaje:** Entender cómo las particiones distribuyen mensajes, cómo la clave determina la partición destino, cómo múltiples consumers se dividen el trabajo y cómo el número de particiones afecta el paralelismo.

**Pre-requisito:** Nivel Uno completado y funcional.

**Nota importante:** El sistema usa `%JAVA_HOME%\bin\java` para ejecutar aplicaciones Java (no solo `java`).

---

## Requisitos previos

- Laboratorio Nivel Uno funcionando correctamente
- Clúster Kafka iniciado (1 broker)
- JAVA_HOME configurado correctamente en Windows

---

## Estructura de directorios a AGREGAR sobre Nivel Uno

kafka-lab-nivel-2/
├── docker/
│   └── (usar el mismo docker-compose.yml del Nivel 1)
├── scripts/
│   ├── powershell/
│   │   ├── 10-crear-topics-particionados.ps1
│   │   ├── 11-describir-particiones.ps1
│   │   ├── 12-producer-con-claves.ps1
│   │   └── 13-consumer-group-multiple.ps1
│   └── cmd/
│       ├── 10-crear-topics-particionados.bat
│       ├── 11-describir-particiones.bat
│       ├── 12-producer-con-claves.bat
│       └── 13-consumer-group-multiple.bat
├── experimentos/
│   ├── exp-02-distribucion-por-clave.md
│   ├── exp-03-paralelismo-consumers.md
│   ├── exp-04-particiones-vs-throughput.md
│   └── resultados/
│       ├── distribucion-mensajes.txt
│       └── asignacion-consumers.txt
├── java/
│   ├── pom.xml (ACTUALIZAR con nuevas clases)
│   └── src/
│       └── main/
│           └── java/
│               └── com/
│                   └── nexus/
│                       └── kafka/
│                           └── nivel2/
│                               ├── KeyedProducer.java
│                               ├── InstrumentedConsumer.java
│                               ├── BatchProducer.java
│                               └── PartitionAnalyzer.java
└── INSTRUCCIONES-NIVEL-2.md

---

## Archivos a generar

### 1. scripts/powershell/10-crear-topics-particionados.ps1

Script que cree tres topics con diferentes configuraciones de particiones:

- `transacciones-1p`: 1 partición, replication-factor 1
- `transacciones-4p`: 4 particiones, replication-factor 1
- `transacciones-8p`: 8 particiones, replication-factor 1

Para cada topic creado, ejecutar kafka-topics --describe para mostrar la distribución de particiones.

Incluir comentarios explicando por qué creamos topics con diferentes números de particiones.

### 2. scripts/powershell/11-describir-particiones.ps1

Script que para cada topic muestre:
- Número de particiones
- Leader de cada partición
- Réplicas (ISR)
- Configuración del topic

Usar `docker exec` para ejecutar kafka-topics --describe dentro del contenedor.

### 3. scripts/powershell/12-producer-con-claves.ps1

Script que ejecute el KeyedProducer 20 veces con diferentes claves de usuario:
- user-001 hasta user-020
- Enviar a topic `transacciones-4p`
- Capturar el output mostrando a qué partición fue cada mensaje
- Guardar resultados en experimentos/resultados/distribucion-mensajes.txt

El script debe pausar 500ms entre cada envío para que sea observable.

### 4. scripts/powershell/13-consumer-group-multiple.ps1

Script que inicie 4 instancias del InstrumentedConsumer en ventanas separadas:
- Todas en el mismo consumer group "grupo-nivel-2"
- Todas suscritas a "transacciones-4p"
- Cada una con un ID único (consumer-1, consumer-2, consumer-3, consumer-4)

Usar `Start-Process powershell -ArgumentList` para abrir nuevas ventanas PowerShell.

Incluir instrucciones de qué observar (cómo se distribuyen las particiones).

### 5. scripts/cmd/*.bat

Versiones equivalentes en batch de todos los scripts PowerShell anteriores.

**IMPORTANTE:** En scripts que ejecuten Java, usar `"%JAVA_HOME%\bin\java"` no `java`.

### 6. java/pom.xml (ACTUALIZADO)

Actualizar el pom.xml del Nivel 1 agregando:
- Nueva dependency: commons-cli para parsing de argumentos de línea de comandos
- Configuración para generar múltiples JARs ejecutables (uno por cada clase main)

### 7. java/.../KeyedProducer.java

Producer instrumentado que:

**Parámetros de entrada:**
- args[0]: topic name
- args[1]: message key (ej: "user-123")
- args[2]: message value (JSON o texto)

**Funcionalidad:**
- Conectar a localhost:9092
- Configurar StringSerializer para key y value
- Configurar acks=all
- ANTES de enviar: calcular y mostrar `hash(key) % num_partitions` teórico
- Enviar el mensaje con la clave especificada
- DESPUÉS de enviar: mostrar partition real, offset, timestamp
- Comparar partition teórica vs real (deberían coincidir)

**Output esperado:**

🔑 Clave del mensaje: user-123
📊 Hash de la clave: 1234567890
📐 Partición teórica (hash % 4): 2
📤 Enviando mensaje...
✅ Mensaje enviado:
Topic: transacciones-4p
Partition: 2 ✓ (coincide con cálculo teórico)
Offset: 15
Timestamp: 1234567890123
Key: user-123
Value: {"monto": 100.00}

Incluir comentarios explicando el algoritmo de particionamiento de Kafka.

### 8. java/.../InstrumentedConsumer.java

Consumer mejorado que:

**Parámetros:**
- args[0]: topic name
- args[1]: group id
- args[2]: consumer id (identificador único para logging)

**Funcionalidad:**
- Configurar auto.offset.reset=earliest
- Suscribirse al topic
- Implementar ConsumerRebalanceListener para detectar cuando se asignan/revocan particiones
- Cuando se asignan particiones: imprimir claramente "🎯 [consumer-X] ASIGNADO A: Partitions [0, 1]"
- Cuando se revocan particiones: imprimir "⚠️ [consumer-X] REVOCADAS: Partitions [0, 1]"
- Para cada mensaje procesado: mostrar consumer-id, partition, offset, key, value
- Incluir contador de mensajes procesados por partition

**Output esperado:**

📊 BatchProducer - Iniciando envío
Topic: transacciones-4p
Mensajes a enviar: 1000
Usar claves: true
⏱️ Enviando mensajes...
[====================] 100% (1000/1000)
✅ Envío completado:
Tiempo total: 2.5 segundos
Throughput: 400 mensajes/segundo
📈 Distribución por partición:
Partition 0: 245 mensajes (24.5%)
Partition 1: 251 mensajes (25.1%)
Partition 2: 248 mensajes (24.8%)
Partition 3: 256 mensajes (25.6%)

### 10. java/.../PartitionAnalyzer.java

Utilidad que analiza un topic y muestra estadísticas:

**Parámetros:**
- args[0]: topic name

**Funcionalidad:**
- Conectar al clúster usando AdminClient
- Describir el topic para obtener número de particiones
- Para cada partición: obtener earliest offset y latest offset
- Calcular número de mensajes en cada partición
- Mostrar tabla de distribución

**Output esperado:**

📊 Análisis del topic: transacciones-4p
┌───────────┬─────────────┬──────────────┬──────────┐
│ Partition │ First Offset│ Last Offset  │ Mensajes │
├───────────┼─────────────┼──────────────┼──────────┤
│     0     │      0      │     245      │   245    │
│     1     │      0      │     251      │   251    │
│     2     │      0      │     248      │   248    │
│     3     │      0      │     256      │   256    │
└───────────┴─────────────┴──────────────┴──────────┘
Total de mensajes: 1000
Distribución: Balanceada ✓

### 11. experimentos/exp-02-distribucion-por-clave.md

**Objetivo:** Demostrar que mensajes con la misma clave siempre van a la misma partición.

**Hipótesis:** Si envío 20 mensajes de "user-123", todos llegarán a la misma partición.

**Procedimiento:**
1. Crear topic `transacciones-4p`
2. Ejecutar KeyedProducer 20 veces con clave "user-123"
3. Ejecutar KeyedProducer 20 veces con clave "user-456"
4. Ejecutar KeyedProducer 20 veces con clave "user-789"
5. Usar PartitionAnalyzer para ver distribución

**Comandos exactos** (usando `%JAVA_HOME%\bin\java`).

**Resultados esperados:** Los 20 mensajes de cada usuario en la misma partición, pero diferentes usuarios pueden estar en particiones diferentes.

**Análisis:** Explicar por qué esto es crítico para mantener orden de eventos por entidad.

### 12. experimentos/exp-03-paralelismo-consumers.md

**Objetivo:** Ver cómo múltiples consumers se dividen las particiones.

**Hipótesis:** Con 4 particiones y 4 consumers, cada consumer procesará exactamente 1 partición.

**Procedimiento:**
1. Usar topic `transacciones-4p` (4 particiones)
2. Ejecutar BatchProducer para llenar el topic con 100 mensajes
3. Iniciar 1 consumer → observar que procesa las 4 particiones
4. Iniciar 2 consumers → observar rebalanceo (2 particiones cada uno)
5. Iniciar 4 consumers → observar rebalanceo (1 partición cada uno)
6. Iniciar 5 consumers → observar que 1 queda idle

**Comandos exactos con `%JAVA_HOME%\bin\java`**.

**Resultados esperados:** Tabla mostrando asignación de particiones en cada escenario.

**Análisis:** Explicar la regla "una partición = un consumer máximo por grupo" y sus implicaciones.

### 13. experimentos/exp-04-particiones-vs-throughput.md

**Objetivo:** Medir cómo el número de particiones afecta el throughput de procesamiento.

**Procedimiento:**
1. Llenar `transacciones-1p` con 1000 mensajes usando BatchProducer
2. Medir tiempo de procesamiento con 1 consumer
3. Llenar `transacciones-4p` con 1000 mensajes
4. Medir tiempo de procesamiento con 4 consumers
5. Llenar `transacciones-8p` con 1000 mensajes
6. Medir tiempo de procesamiento con 8 consumers

**Comandos exactos**.

**Resultados esperados:** Tabla comparativa mostrando que más particiones + más consumers = mayor throughput total.

**Análisis:** Explicar el límite del paralelismo y cuándo más particiones ya no ayudan.

### 14. INSTRUCCIONES-NIVEL-2.md

Documento paso a paso para completar el Nivel Dos:

**Sección 1: Preparación**
- Verificar que Nivel Uno funciona
- Iniciar el clúster

**Sección 2: Creación de topics particionados**
- Ejecutar script 10-crear-topics-particionados.ps1
- Verificar con script 11-describir-particiones.ps1

**Sección 3: Compilación del código**
- Comando Maven para compilar nuevas clases
- Verificación de JARs generados
- **RECORDATORIO:** Usar `%JAVA_HOME%\bin\java` en todos los comandos

**Sección 4: Experimento 02 - Distribución por clave**
- Seguir exp-02-distribucion-por-clave.md paso a paso
- Capturar screenshots de resultados

**Sección 5: Experimento 03 - Paralelismo**
- Seguir exp-03-paralelismo-consumers.md
- Observar ventanas de consumers en tiempo real

**Sección 6: Experimento 04 - Throughput**
- Seguir exp-04-particiones-vs-throughput.md
- Comparar resultados

**Sección 7: Limpieza**
- Detener consumers (Ctrl+C en cada ventana)
- Opcionalmente eliminar topics creados

**Sección 8: Próximos pasos**
- Adelanto del Nivel 3: clúster multi-broker

---

## Validaciones importantes

- Todos los comandos Java deben usar `"%JAVA_HOME%\bin\java"` con comillas
- Scripts PowerShell deben verificar que JAVA_HOME existe antes de ejecutar
- Scripts batch deben hacer lo mismo
- Mensajes de error claros si JAVA_HOME no está configurado

---

## Formato de output

- Scripts con encoding UTF-8
- Código Java con comentarios abundantes
- Documentos .md con ejemplos de output reales
- Tablas en formato legible

---

## Checklist de validación

Generar archivo VALIDACION-NIVEL-2.md:

- [ ] Topics con 1, 4 y 8 particiones creados
- [ ] KeyedProducer funciona correctamente
- [ ] InstrumentedConsumer muestra asignación de particiones
- [ ] BatchProducer envía mensajes exitosamente
- [ ] PartitionAnalyzer muestra estadísticas correctas
- [ ] Experimento 02 completado - misma clave va a misma partición
- [ ] Experimento 03 completado - entiendo asignación de consumers
- [ ] Experimento 04 completado - veo impacto en throughput
- [ ] Entiendo cómo las particiones habilitan paralelismo