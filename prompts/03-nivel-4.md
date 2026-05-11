# LABORATORIO KAFKA - NIVEL 4: Réplicas y Tolerancia a Fallos

## Contexto del proyecto

Este es el Nivel Cuatro del laboratorio Kafka, donde finalmente implementamos el mecanismo que hace a Kafka verdaderamente robusto y confiable para entornos de producción: la replicación de particiones. Hasta ahora has trabajado con particiones que existen solo en un broker (replication-factor igual a uno), lo cual significa que si ese broker falla, pierdes los datos de esa partición. En este nivel configuraremos réplicas múltiples, simularemos fallos de brokers y observarás cómo Kafka continúa operando sin pérdida de datos ni tiempo de inactividad significativo.

**Objetivo de aprendizaje:** Entender profundamente cómo funcionan las réplicas en Kafka, qué significa que una réplica esté in-sync, cómo Kafka elige automáticamente nuevos leaders cuando el leader actual falla, y cómo la configuración de acknowledgments en el producer afecta la durabilidad versus el rendimiento. Al completar este nivel, serás capaz de diseñar topics con el nivel apropiado de redundancia para diferentes casos de uso de producción.

**Pre-requisito:** Nivel Tres completado y entendido. Debes tener claro cómo funcionan las particiones, cómo se distribuyen los leaders entre brokers y cómo múltiples brokers mejoran el throughput. El concepto de leader de partición debe estar completamente claro porque ahora agregaremos followers que replican al leader.

**Nota importante:** Este nivel usa el mismo clúster de tres brokers del Nivel Tres, pero ahora crearemos topics con factor de replicación mayor a uno. Seguimos usando Kafka cuatro punto cero con KRaft y manteniendo compatibilidad total con Windows diez usando la variable de entorno JAVA_HOME para ejecutar aplicaciones.

---

## Requisitos previos

Antes de comenzar el Nivel Cuatro, asegúrate de que el Nivel Tres está completamente funcional. El clúster de tres brokers debe iniciar sin errores y los tres brokers deben reconocerse entre sí. Docker Desktop necesita tener al menos seis gigabytes de RAM asignados porque continuaremos usando tres brokers simultáneamente. Verifica que puedes crear topics particionados y que entiendes cómo se distribuyen los leaders. Las aplicaciones Java del Nivel Tres deben compilar y ejecutarse correctamente porque reutilizaremos algunas de ellas y crearemos nuevas versiones mejoradas.

Un concepto fundamental que debes tener claro antes de empezar es la diferencia entre partición y réplica. Una partición es una secuencia ordenada de mensajes. Una réplica es una copia completa de esa partición. Si tienes una partición con factor de replicación tres, significa que existen tres copias idénticas de esa partición en tres brokers diferentes. Una de esas copias es el leader que maneja todas las lecturas y escrituras, mientras que las otras dos son followers que se sincronizan constantemente con el leader pero no sirven tráfico de clients directamente.

---

## Estructura de directorios a AGREGAR sobre niveles anteriores

La estructura del Nivel Cuatro se integra completamente con lo que ya construiste. No reemplazamos archivos anteriores sino que agregamos nuevos componentes enfocados en replicación y tolerancia a fallos. Los scripts y experimentos anteriores siguen siendo válidos y útiles para comparar comportamiento con y sin replicación.

kafka-lab-nivel-4/
├── docker/
│   └── (usar el mismo docker-compose-cluster.yml del Nivel 3)
├── scripts/
│   ├── powershell/
│   │   ├── 30-crear-topics-replicados.ps1
│   │   ├── 31-describir-replicas.ps1
│   │   ├── 32-simular-fallo-broker.ps1
│   │   ├── 33-recuperar-broker.ps1
│   │   ├── 34-verificar-isr.ps1
│   │   └── 35-monitorear-under-replicated.ps1
│   └── cmd/
│       ├── 30-crear-topics-replicados.bat
│       ├── 31-describir-replicas.bat
│       ├── 32-simular-fallo-broker.bat
│       ├── 33-recuperar-broker.bat
│       ├── 34-verificar-isr.bat
│       └── 35-monitorear-under-replicated.bat
├── experimentos/
│   ├── exp-08-replicas-basicas.md
│   ├── exp-09-fallo-leader.md
│   ├── exp-10-durabilidad-acks.md
│   ├── exp-11-sincronizacion-replicas.md
│   └── resultados/
│       ├── estado-replicas-antes-fallo.txt
│       ├── estado-replicas-durante-fallo.txt
│       ├── estado-replicas-despues-recuperacion.txt
│       └── comparacion-acks.txt
├── java/
│   ├── pom.xml (ACTUALIZAR)
│   └── src/
│       └── main/
│           └── java/
│               └── com/
│                   └── nexus/
│                       └── kafka/
│                           └── nivel4/
│                               ├── ReplicaAnalyzer.java
│                               ├── DurableProducer.java
│                               ├── FailoverMonitor.java
│                               └── ISRTracker.java
└── INSTRUCCIONES-NIVEL-4.md

---

## Archivos a generar

### 1. scripts/powershell/30-crear-topics-replicados.ps1

Este script crea una serie de topics con diferentes configuraciones de replicación para experimentación. La diferencia clave con los topics que has creado antes es que ahora el parámetro replication-factor será mayor a uno, lo que significa que cada partición tendrá múltiples copias en diferentes brokers.

El script debe crear los siguientes topics específicamente diseñados para demostrar diferentes aspectos de la replicación. Primero, un topic llamado "transacciones-rf1" con cuatro particiones y factor de replicación uno. Este sirve como línea base para comparar contra topics replicados. Segundo, un topic llamado "transacciones-rf2" con cuatro particiones y factor de replicación dos. Aquí cada partición existe en dos brokers, uno como leader y otro como follower. Tercero, un topic llamado "transacciones-rf3" con cuatro particiones y factor de replicación tres. Este es el máximo factor de replicación posible con tres brokers, cada partición existe en los tres brokers simultáneamente.

Además, crea un topic especial llamado "critical-data" con dos particiones y factor de replicación tres, configurado con min.insync.replicas igual a dos. Esta configuración especial requiere que al menos dos réplicas confirmen cada escritura antes de considerarla exitosa, proporcionando un nivel adicional de durabilidad a costa de un pequeño impacto en latencia.

Para cada topic creado, el script debe inmediatamente ejecutar kafka-topics --describe para mostrar cómo se distribuyeron las réplicas. La salida del comando describe es particularmente importante ahora porque mostrará no solo el leader de cada partición sino también la lista completa de réplicas y cuáles están in-sync.

Incluye comentarios extensos explicando qué significa cada configuración. Por ejemplo, explica que replication-factor tres con tres brokers significa que si pierdes dos brokers simultáneamente todavía tienes una copia de los datos y el clúster puede seguir operando. Explica que min.insync.replicas dos significa que el producer no considerará una escritura exitosa hasta que al menos dos de las tres réplicas hayan confirmado que escribieron el mensaje, proporcionando mayor garantía de durabilidad.

El script debe también mostrar un resumen visual de la distribución de réplicas. Para cada topic, cuenta cuántas réplicas totales hay en cada broker (sumando todas las particiones) y muestra si la distribución es balanceada. Kafka intenta distribuir réplicas uniformemente entre brokers para evitar que un broker almacene más datos que otros.

Guarda la salida completa del comando describe de cada topic en archivos separados en experimentos/resultados/ para poder comparar el estado antes y después de simular fallos. Nombra estos archivos como "topic-[nombre]-estado-inicial.txt" para referencia futura.

### 2. scripts/powershell/31-describir-replicas.ps1

Este script proporciona un análisis detallado y visual del estado de las réplicas en el clúster. Es más sofisticado que un simple kafka-topics --describe porque parsea la información y la presenta de forma clara y accionable.

Acepta un parámetro opcional de nombre de topic. Si no se proporciona, analiza todos los topics replicados en el clúster. Para cada topic, extrae la siguiente información crítica de cada partición: quién es el leader actual, cuáles son todas las réplicas (la lista completa de brokers que tienen una copia de esta partición), y cuáles réplicas están actualmente in-sync (ISR, que significa que están completamente sincronizadas con el leader).

La diferencia entre réplicas totales e ISR es crucial para entender la salud del clúster. Si todas las réplicas están in-sync, el clúster está saludable y todas las copias están al día. Si hay réplicas que no están in-sync, significa que esas copias están retrasadas respecto al leader, lo cual puede indicar problemas de red, broker sobrecargado o broker que se está recuperando de un fallo.

Crea una visualización clara mostrando cada partición con su estado. Por ejemplo, para una partición específica muestra algo como esto:

Topic: transacciones-rf3, Partition: 0
Leader: Broker 2 ⭐
Réplicas: [2, 3, 1]
ISR: [2, 3, 1] ✅ (todas las réplicas in-sync)
Estado: SALUDABLE
Topic: transacciones-rf3, Partition: 1
Leader: Broker 3 ⭐
Réplicas: [3, 1, 2]
ISR: [3, 1] ⚠️ (Broker 2 está fuera de sincronización)
Estado: DEGRADADO - Falta sincronizar 1 réplica

Implementa lógica para detectar automáticamente particiones con problemas. Una partición está degradada si tiene réplicas que no están in-sync. Una partición está en riesgo crítico si solo tiene una réplica in-sync (el leader) porque eso significa que si el leader falla, la partición quedará inaccesible temporalmente hasta que se sincronice otra réplica.

Calcula estadísticas globales del clúster. Muestra cuántas particiones totales hay, cuántas están completamente saludables (todas las réplicas in-sync), cuántas están degradadas (algunas réplicas fuera de sync) y cuántas están en riesgo crítico (solo el leader in-sync). Esto da una visión inmediata de la salud general del clúster.

Incluye una sección que muestra la distribución de réplicas por broker. Para cada broker, cuenta cuántas réplicas totales almacena (como leader o follower) y cuántas de esas réplicas están actualmente in-sync. Esto ayuda a identificar si algún broker está sobrecargado o teniendo problemas de sincronización.

El script debe guardar un snapshot completo del estado actual en experimentos/resultados/ con timestamp para que puedas comparar el estado del clúster antes y después de eventos como fallos de brokers o recuperaciones.

### 3. scripts/powershell/32-simular-fallo-broker.ps1

Este script simula la falla de un broker de forma controlada para que puedas observar cómo Kafka maneja el failover automático. Es uno de los experimentos más importantes del laboratorio porque demuestra la robustez del sistema.

Acepta un parámetro que especifica qué broker simular como fallido: uno, dos o tres. Antes de detener el broker, el script debe ejecutar validaciones importantes. Primero, verifica que hay topics replicados en el clúster, porque simular fallo de un broker sin replicación solo demostrará pérdida de datos, que no es el objetivo. Segundo, ejecuta el script treinta y uno para capturar el estado completo de las réplicas antes del fallo. Guarda esta información en experimentos/resultados/estado-replicas-antes-fallo.txt.

Muestra una advertencia clara al usuario explicando qué va a suceder. Por ejemplo: "Este script detendrá el broker dos simulando un fallo catastrófico. Topics con replication-factor mayor a uno continuarán operando, pero particiones donde el broker dos es el único leader experimentarán failover automático a otro broker. El proceso típicamente toma entre cinco y diez segundos."

Pide confirmación explícita del usuario antes de proceder. Si el usuario confirma, detén el broker especificado usando docker stop. Importante: usa docker stop sin el parámetro -t para que envíe una señal de terminación abrupta simulando un fallo real, no un shutdown graceful.

Inmediatamente después de detener el broker, inicia un loop de monitoreo que ejecuta cada dos segundos durante treinta segundos. En cada iteración, verifica el estado del clúster ejecutando kafka-metadata dentro de uno de los brokers que sigue corriendo. Busca evidencia de que Kafka detectó el fallo y comenzó el proceso de elección de nuevos leaders.

Específicamente, monitorea los cambios en los leaders de particiones. Para cada partición que tenía su leader en el broker que acabas de detener, Kafka debe haber elegido automáticamente un nuevo leader de entre los followers que estaban in-sync. Muestra estos cambios claramente: "Partition cero del topic transacciones-rf3: leader cambió de Broker dos a Broker tres".

Después de treinta segundos, ejecuta nuevamente el script treinta y uno para capturar el estado completo después del fallo. Guarda esto en experimentos/resultados/estado-replicas-durante-fallo.txt. Compara automáticamente este estado con el estado previo al fallo para generar un reporte de cambios.

El reporte debe mostrar para cada topic: cuántas particiones cambiaron de leader, cuántas réplicas están ahora fuera de sync (porque el broker caído tenía copias de esas particiones), y si hay alguna partición que quedó inaccesible (esto solo debería pasar si tenías particiones sin replicación adecuada).

Muestra instrucciones claras de qué hacer después. Por ejemplo: "El broker dos está detenido simulando un fallo. El clúster continúa operando con los brokers uno y tres. Para recuperar el broker y observar el proceso de re-sincronización, ejecuta el script treinta y tres-recuperar-broker.ps1 especificando el broker dos."

Incluye manejo de errores robusto. Si intentas detener un broker que ya está detenido, muestra un mensaje apropiado. Si el clúster no puede manejar el fallo (por ejemplo, si detienes el único broker que tiene copias de ciertas particiones), detecta esta situación y advierte al usuario.

### 4. scripts/powershell/33-recuperar-broker.ps1

Este script complementa al anterior, recuperando un broker previamente detenido y monitoreando el proceso de re-sincronización de sus réplicas con los brokers que siguieron operando durante su ausencia.

Acepta como parámetro qué broker recuperar. Verifica primero que ese broker efectivamente está detenido. Si intentas recuperar un broker que ya está corriendo, muestra un mensaje indicando que no hay nada que hacer.

Antes de iniciar el broker, explica al usuario qué va a suceder: "Cuando el broker se reinicie, Kafka detectará automáticamente que está de vuelta. El broker comenzará a sincronizar sus réplicas con los leaders actuales para ponerse al día con cualquier mensaje que se escribió mientras estuvo caído. Dependiendo de cuántos mensajes se escribieron, este proceso puede tomar desde unos segundos hasta varios minutos."

Inicia el broker usando docker start. Espera unos segundos para que el proceso de Kafka dentro del contenedor se inicialice completamente.

Implementa un loop de monitoreo que observa el progreso de la re-sincronización. Cada cinco segundos durante dos minutos, ejecuta el script treinta y cuatro-verificar-isr.ps1 internamente para ver cuántas réplicas del broker recuperado se han re-sincronizado y se agregaron nuevamente al ISR.

Muestra progreso visual. Por ejemplo: "Re-sincronización en progreso... Broker dos tiene ocho réplicas. Estado actual: cinco in-sync, tres sincronizando. Progreso: sesenta y dos punto cinco por ciento".

Detecta cuando la re-sincronización está completa. Esto sucede cuando todas las réplicas que deberían estar en el broker recuperado aparecen nuevamente en el ISR de sus respectivas particiones. Cuando esto ocurre, muestra un mensaje de éxito: "Re-sincronización completada. El broker dos está completamente recuperado y todas sus réplicas están in-sync nuevamente."

Verifica si Kafka rebalanceó automáticamente algunos leaders de vuelta al broker recuperado. En algunos casos Kafka puede preferir mantener los leaders donde están actualmente aunque el broker original haya vuelto, para evitar interrupciones innecesarias. En otros casos, especialmente si hay configuración de preferred leaders, Kafka puede mover leaders de vuelta a sus posiciones preferidas.

Captura el estado final del clúster después de la recuperación completa y guárdalo en experimentos/resultados/estado-replicas-despues-recuperacion.txt. Genera un reporte comparativo mostrando cómo el estado antes del fallo, durante el fallo y después de la recuperación difieren.

Incluye una sección de troubleshooting. Si la re-sincronización no completa en el tiempo esperado, sugiere verificar los logs del broker recuperado con docker logs para identificar posibles problemas. Proporciona comandos específicos para diagnosticar problemas comunes como errores de conectividad o corrupción de datos.

### 5. scripts/powershell/34-verificar-isr.ps1

Este script está enfocado específicamente en monitorear el estado del ISR (In-Sync Replicas) de todos los topics. El ISR es uno de los conceptos más importantes en la tolerancia a fallos de Kafka, y este script lo hace visible y comprensible.

Para cada partición de cada topic, compara la lista de réplicas configuradas con la lista de réplicas actualmente in-sync. En un clúster saludable, estas dos listas deberían ser idénticas. Cuando difieren, indica un problema que necesita atención.

Categoriza las particiones según su estado de ISR. Una partición está en estado verde (completamente saludable) si todas sus réplicas están in-sync. Una partición está en estado amarillo (advertencia) si tiene al menos dos réplicas in-sync pero falta alguna. Una partición está en estado rojo (crítico) si solo tiene una réplica in-sync, que debe ser el leader, porque esto significa que no hay redundancia activa en este momento.

Genera un reporte visual usando colores en PowerShell. Muestra cada categoría claramente:

Estado del ISR en el clúster
🟢 PARTICIONES SALUDABLES (18 particiones)
Todas las réplicas in-sync, sin problemas
🟡 PARTICIONES CON ADVERTENCIA (3 particiones)
Topic: transacciones-rf3, Partition: 1
Réplicas configuradas: [1, 2, 3]
ISR actual: [1, 3]
Faltante: Broker 2
Posible causa: Broker caído o muy retrasado
🔴 PARTICIONES CRÍTICAS (1 partición)
Topic: transacciones-rf2, Partition: 0
Réplicas configuradas: [2, 3]
ISR actual: [2]
Faltante: Broker 3
RIESGO: Si el broker 2 falla, esta partición será inaccesible

Para réplicas que están fuera de sync, intenta determinar por qué. Verifica si el broker que tiene esa réplica está corriendo con docker ps. Si el broker está detenido, ese es claramente el motivo. Si el broker está corriendo pero la réplica no está in-sync, sugiere verificar los logs del broker para identificar si hay problemas de red, disco lleno o el broker está sobrecargado y no puede mantener el ritmo de replicación.

Calcula métricas agregadas. Muestra el porcentaje total de particiones que están completamente saludables. Un clúster de producción debería mantener este porcentaje cerca del cien por ciento la mayor parte del tiempo.

Implementa un modo de monitoreo continuo con el parámetro -Watch. Cuando se activa, el script se ejecuta en loop cada diez segundos, limpiando la pantalla y mostrando el estado actualizado del ISR. Esto es útil cuando estás observando el proceso de re-sincronización después de recuperar un broker o cuando estás produciendo mensajes activamente y quieres ver cómo se mantiene la sincronización en tiempo real.

Guarda snapshots históricos del estado del ISR con timestamps. Esto te permite después analizar cuánto tiempo tomó la re-sincronización o identificar patrones de réplicas que frecuentemente caen fuera de sync, lo cual podría indicar problemas de infraestructura.

### 6. scripts/powershell/35-monitorear-under-replicated.ps1

Kafka tiene una métrica específica llamada "under-replicated partitions" que indica particiones cuyo número de réplicas in-sync es menor al número de réplicas configuradas. Este script monitorea específicamente esta métrica porque es un indicador clave de la salud del clúster.

El script consulta las métricas JMX de cada broker para obtener el contador de particiones under-replicated. Aunque acceder a JMX desde PowerShell es complejo, puedes parsear la salida de comandos de Kafka que reportan esta información o usar kafka-run-class para ejecutar herramientas de métricas.

Alternativamente, puedes implementar esta funcionalidad parseando la salida de kafka-topics --describe y contando manualmente cuántas particiones tienen menos réplicas in-sync que réplicas totales. Este enfoque es menos eficiente pero más portable y no requiere acceso JMX.

Para cada partición under-replicated encontrada, muestra información detallada: qué topic y partición es, cuántas réplicas debería tener, cuántas tiene actualmente in-sync, y qué brokers tienen las réplicas faltantes.

Calcula el tiempo que cada partición ha estado under-replicated si es posible. Si has estado guardando snapshots históricos del estado del ISR, puedes comparar con estados previos para determinar cuánto tiempo ha pasado desde que la partición cayó fuera de sincronización completa.

Implementa alertas basadas en umbrales. Si el número total de particiones under-replicated excede un cierto porcentaje del total de particiones (por ejemplo, cinco por ciento), muestra una advertencia prominente indicando que el clúster necesita atención. Si el número excede un umbral crítico (por ejemplo, veinte por ciento), muestra una alerta severa.

Proporciona recomendaciones accionables. Si detectas particiones under-replicated, sugiere pasos específicos para diagnosticar y resolver el problema. Por ejemplo: "Tres particiones están under-replicated porque el broker dos está detenido. Ejecuta el script treinta y tres-recuperar-broker.ps1 para reiniciar el broker dos y re-sincronizar sus réplicas."

Genera reportes históricos. Guarda el estado de under-replicated partitions periódicamente en archivos con timestamp. Esto te permite después generar gráficas o análisis de tendencias para ver si el problema está mejorando o empeorando con el tiempo.

### 7. scripts/cmd/*.bat

Genera versiones batch equivalentes de todos los scripts PowerShell del Nivel Cuatro. Estos scripts deben proporcionar la misma funcionalidad pero usando sintaxis de Windows batch.

Los scripts batch son inherentemente más limitados que PowerShell para tareas complejas como parsear JSON o XML, así que la implementación puede ser más simple pero debe cubrir los casos de uso principales. Enfócate en la funcionalidad core de cada script.

Para crear topics replicados desde batch, los comandos docker exec son idénticos a PowerShell, solo cambia la sintaxis de escape y continuación de líneas. Usa el carácter circunflejo (^) para escapar caracteres especiales y para continuar comandos en múltiples líneas.

Para monitoreo y verificación, captura la salida de comandos en archivos temporales y luego usa findstr o find para parsear información relevante. Por ejemplo, para verificar ISR, ejecuta kafka-topics --describe, guarda la salida en un archivo temporal, y usa findstr para extraer líneas que contienen "Isr".

Los colores en batch son más complicados que en PowerShell. Puedes usar comandos especiales de color si están disponibles, o simplemente omitir los colores y enfocarte en contenido claro. La funcionalidad es más importante que la presentación visual en batch.

Asegúrate de que todos los comandos Java usen "%JAVA_HOME%\bin\java" con comillas para manejar correctamente espacios en rutas. Verifica que JAVA_HOME existe antes de ejecutar comandos que dependen de Java.

Incluye comentarios REM extensos explicando qué hace cada sección del script, porque la sintaxis batch puede ser menos intuitiva que PowerShell para desarrolladores modernos.

### 8. java/pom.xml (ACTUALIZADO)

Actualiza el archivo pom.xml para incluir las nuevas clases del Nivel Cuatro. Las dependencias existentes de kafka-clients deberían ser suficientes para la funcionalidad de este nivel.

Incrementa la versión del proyecto a uno punto cuatro punto cero para reflejar que estamos en el Nivel Cuatro.

Asegúrate de que el plugin maven-shade está configurado para generar JARs ejecutables de las nuevas clases que tienen método main: ReplicaAnalyzer, DurableProducer, FailoverMonitor e ISRTracker.

Agrega comentarios indicando qué clases nuevas se introdujeron en este nivel y su propósito específico. Por ejemplo: "ReplicaAnalyzer proporciona análisis programático del estado de réplicas e ISR similar al script treinta y uno pero desde código Java."

### 9. java/.../ReplicaAnalyzer.java

Esta clase Java proporciona análisis programático profundo del estado de replicación en el clúster. Es similar al script treinta y uno pero implementado en Java, lo que te permite integrar este análisis en aplicaciones más complejas o frameworks de monitoreo.

Usa AdminClient de Kafka para conectarse al clúster. Configura bootstrap servers con los tres brokers. Implementa métodos para extraer información detallada sobre réplicas e ISR de todos los topics o de un topic específico.

El método principal debe ser analyzeReplicationState que acepta un nombre de topic opcional. Si se proporciona, analiza solo ese topic. Si no, analiza todos los topics en el clúster. Para cada topic, usa describeTopics de AdminClient para obtener información completa de cada partición.

Para cada partición, extrae la información crítica de replicación. Primero, identifica el nodo líder actual usando partition.leader(). Segundo, obtén la lista completa de réplicas usando partition.replicas(), que devuelve una lista de nodos que deberían tener copias de esta partición. Tercero, obtén el ISR actual usando partition.isr(), que devuelve solo los nodos que están actualmente sincronizados con el leader.

Implementa lógica de comparación entre réplicas configuradas e ISR. Crea un método isFullyReplicated que devuelve verdadero solo si todos los nodos en la lista de réplicas también están en el ISR. Crea otro método getOutOfSyncReplicas que devuelve la diferencia entre réplicas configuradas e ISR, identificando exactamente qué brokers tienen copias retrasadas.

Genera un objeto de reporte estructurado en lugar de solo imprimir a consola. Define clases internas como TopicReplicationReport, PartitionReplicationState, etcétera, que contengan toda la información estructurada. Esto permite que el código que llama a tu analyzer procese los resultados programáticamente.

El reporte debe incluir métricas agregadas. Cuenta cuántas particiones totales hay en el clúster, cuántas están completamente replicadas, cuántas están parcialmente replicadas (algunas réplicas fuera de sync), y cuántas están en riesgo crítico (solo el leader in-sync).

Implementa visualización en consola cuando la clase se ejecuta standalone. El método main debe aceptar argumentos de línea de comandos para especificar el topic a analizar. Si no se proporciona topic, analiza todo el clúster. Genera una salida formateada similar a la del script PowerShell:

╔════════════════════════════════════════════════════════╗
║     ANÁLISIS DE REPLICACIÓN - CLÚSTER KAFKA            ║
╚════════════════════════════════════════════════════════╝
Resumen global:
• Total de particiones: 24
• Completamente replicadas: 21 (87.5%)
• Parcialmente replicadas: 3 (12.5%)
• Riesgo crítico: 0 (0%)
─────────────────────────────────────────────────────────
Topic: transacciones-rf3 (4 particiones, RF=3)
✅ Partition 0
Leader: Broker 2
Réplicas: [2, 3, 1]
ISR: [2, 3, 1]
Estado: COMPLETAMENTE REPLICADO
⚠️  Partition 1
Leader: Broker 3
Réplicas: [3, 1, 2]
ISR: [3, 1]
Fuera de sync: [2]
Estado: PARCIALMENTE REPLICADO
✅ Partition 2
Leader: Broker 1
Réplicas: [1, 2, 3]
ISR: [1, 2, 3]
Estado: COMPLETAMENTE REPLICADO
✅ Partition 3
Leader: Broker 2
Réplicas: [2, 1, 3]
ISR: [2, 1, 3]
Estado: COMPLETAMENTE REPLICADO

Implementa un modo de exportación que guarda los resultados en formato JSON para procesamiento posterior o integración con sistemas de monitoreo. Usa una librería JSON simple de Jackson o Gson que ya deberías tener en tu classpath a través de dependencias transitivas de Kafka.

Incluye manejo robusto de errores. Si el clúster no está disponible, captura la excepción y muestra un mensaje claro. Si un topic específico no existe, informa de eso sin stacktrace críptico. Usa bloques try-catch apropiados alrededor de las llamadas a AdminClient.

Implementa un método comparativo compareReplicationStates que acepta dos snapshots de estado de replicación (por ejemplo, uno antes de un fallo y otro después) y genera un reporte de diferencias mostrando qué cambió. Esto es útil para análisis post-mortem de incidentes.

### 10. java/.../DurableProducer.java

Este producer está específicamente diseñado para demostrar cómo diferentes configuraciones de acknowledgment afectan la durabilidad y el rendimiento. Es una herramienta educativa para entender el trade-off entre velocidad y garantías de durabilidad.

Acepta parámetros de línea de comandos que especifican el topic, número de mensajes a enviar, y nivel de acknowledgment deseado. Los niveles de acks soportados son: cero (no esperar confirmación del broker, máxima velocidad pero sin garantía de durabilidad), uno (esperar confirmación solo del leader, balance entre velocidad y durabilidad), y all o menos uno (esperar confirmación del leader y todos los in-sync replicas, máxima durabilidad pero posiblemente más lento).

Configura el KafkaProducer con el nivel de acks especificado. Además, configura enable.idempotence en verdadero para evitar duplicados en caso de reintentos. Configura max.in.flight.requests.per.connection apropiadamente según el nivel de acks para mantener el orden de mensajes.

Antes de enviar mensajes, muestra claramente qué configuración está usando y qué garantías proporciona cada nivel de acks. Por ejemplo:

🔧 Configuración del DurableProducer
Topic: critical-data
Mensajes a enviar: 1000
Nivel de ACKs: all
📋 Garantías con acks=all:
• El producer espera confirmación del leader Y todos los ISR
• Si el leader falla inmediatamente después de confirmar, los datos
están garantizados en al menos min.insync.replicas brokers
• Este es el nivel más seguro pero puede tener mayor latencia
• Recomendado para datos críticos que no pueden perderse


Mientras envías mensajes, mide tanto el throughput como la latencia de cada send. Usa callbacks para el método send que registran exactamente cuánto tiempo tomó desde que enviaste el mensaje hasta que recibiste confirmación del broker. Acumula estas latencias para calcular estadísticas al final.

Implementa simulación de fallo durante el envío si se especifica un parámetro especial. Por ejemplo, después de enviar quinientos mensajes, el programa puede instruir al usuario a ejecutar el script de simular fallo de broker, luego continuar enviando más mensajes. Esto demuestra cómo different acks levels manejan fallos de brokers durante producción activa.

Con acks igual a cero, los mensajes enviados después del fallo podrían perderse silenciosamente si fueron al broker que falló. Con acks igual a uno, el producer recibirá errores cuando intente enviar al broker fallido si ese broker era el leader, y puede reintentar automáticamente. Con acks igual a all, el producer tiene la máxima garantía de que los mensajes confirmados están en múltiples réplicas, así que incluso si el leader falla, los mensajes están seguros.

Al finalizar el envío de todos los mensajes, genera un reporte comparativo si ejecutaste con diferentes niveles de acks. Muestra el throughput y latencia promedio de cada configuración:


📊 Resultados del benchmark de durabilidad
acks=0 (sin confirmación):
• Throughput: 15,000 mensajes/segundo
• Latencia promedio: 2.3 ms
• Latencia p95: 4.1 ms
• Mensajes potencialmente perdidos en fallo: Desconocido
acks=1 (solo leader):
• Throughput: 8,500 mensajes/segundo
• Latencia promedio: 8.7 ms
• Latencia p95: 15.2 ms
• Mensajes potencialmente perdidos en fallo: 0 (detectados y reinten
tados)
acks=all (leader + ISR):
• Throughput: 6,200 mensajes/segundo
• Latencia promedio: 12.4 ms
• Latencia p95: 21.8 ms
• Mensajes potencialmente perdidos en fallo: 0 (garantizados en múltiples réplicas)
💡 Conclusión:
acks=all sacrifica ~58% de throughput vs acks=0, pero proporciona
garantía absoluta de durabilidad. Para datos críticos, este trade-off
generalmente vale la pena.

Guarda los resultados de cada ejecución en archivos CSV en experimentos/resultados/ con timestamp y configuración utilizada. Esto permite análisis posterior y comparación de diferentes configuraciones.

Implementa verificación posterior al envío. Después de enviar todos los mensajes, usa un consumer para leer el topic y contar cuántos mensajes realmente se guardaron. Compara este número con el número de mensajes que el producer cree que envió exitosamente. Con acks cero, estos números pueden diferir. Con acks all, deberían coincidir exactamente.

### 11. java/.../FailoverMonitor.java

Esta aplicación monitorea el clúster en tiempo real y detecta automáticamente eventos de failover cuando ocurren. Es especialmente útil para correr en una ventana separada mientras ejecutas los experimentos de simulación de fallos.

Usa AdminClient para conectarse al clúster. Implementa un loop infinito que cada dos segundos consulta el estado del clúster y compara con el estado previo para detectar cambios.

Para cada topic monitoreado, rastrea quién es el leader de cada partición. Cuando el leader de una partición cambia, esto indica que ocurrió un failover. Detecta este cambio y registra el evento con timestamp preciso, partición afectada, leader anterior y nuevo leader.

Implementa detección de cambios en el ISR. Cuando una réplica se agrega o remueve del ISR, esto indica que una réplica se sincronizó o se desincronizó. Registra estos eventos porque son indicadores importantes de la salud del clúster.

Detecta cuando un broker se une o se va del clúster. Esto sucede cuando detienes o inicias un broker. Cuando un broker se va, registra todos los impactos: qué particiones perdieron su leader, qué particiones ahora tienen menos réplicas in-sync, etcétera.

Muestra eventos en tiempo real en la consola con formato claro y colores. Usa símbolos visuales para diferentes tipos de eventos:

⏰ 14:23:45.123 - Monitor iniciado, rastreando 3 topics
✅ 14:23:47.456 - Estado inicial del clúster capturado
• 3 brokers activos
• 12 particiones monitoreadas
• 36 réplicas totales, todas in-sync
⚠️  14:24:15.789 - BROKER CAÍDO DETECTADO
• Broker 2 ya no responde
• 4 particiones impactadas
🔄 14:24:16.234 - FAILOVER DETECTADO
• Topic: transacciones-rf3, Partition: 0
• Leader cambió: Broker 2 → Broker 3
• Tiempo de failover: 445 ms
🔄 14:24:16.567 - FAILOVER DETECTADO
• Topic: transacciones-rf3, Partition: 1
• Leader cambió: Broker 2 → Broker 1
• Tiempo de failover: 778 ms
📉 14:24:16.890 - CAMBIO EN ISR
• Topic: critical-data, Partition: 0
• ISR antes: [1, 2, 3]
• ISR ahora: [1, 3]
• Réplica removida: Broker 2
⏱️  14:24:17.123 - Resumen de impacto del fallo:
• Tiempo total de failover: 1.334 segundos
• Particiones que cambiaron leader: 4
• Particiones ahora under-replicated: 8
• Sistema sigue operacional: ✅


Implementa acumulación de estadísticas de failover. Cuando detectas un evento de failover, mide cuánto tiempo tomó desde que el broker se volvió inaccesible hasta que Kafka eligió un nuevo leader. Acumula estas mediciones para calcular tiempos promedio y máximo de failover.

Registra todos los eventos en un archivo de log estructurado además de mostrarlos en consola. Usa un formato fácil de parsear como CSV o JSON para que puedas después analizar los logs con herramientas o scripts.

Implementa un modo de alerta. Si el tiempo de failover excede un umbral configurado (por ejemplo, cinco segundos), muestra una advertencia prominente porque failovers lentos pueden indicar problemas de configuración o hardware.

Cuando el monitor detecta que un broker previamente caído ha vuelto, rastrea el proceso de re-sincronización. Muestra cuántas réplicas necesita sincronizar el broker recuperado y reporta progreso a medida que las réplicas se agregan de vuelta al ISR.

Permite configurar qué topics monitorear. Si no se especifica, monitorea todos los topics replicados en el clúster. Si se especifican topics específicos, enfócate solo en esos para reducir ruido en el output.

### 12. java/.../ISRTracker.java

Esta utilidad está dedicada específicamente a rastrear el estado del ISR en el tiempo y generar reportes históricos. Es complementaria al FailoverMonitor pero enfocada en tendencias a largo plazo en lugar de eventos en tiempo real.

Implementa dos modos de operación: snapshot mode y tracking mode. En snapshot mode, toma una fotografía única del estado actual del ISR de todos los topics y la guarda en un archivo con timestamp. En tracking mode, toma snapshots periódicamente (por ejemplo, cada treinta segundos) y los acumula en una base de datos simple o archivo CSV para análisis histórico.

Para cada snapshot, registra información completa de cada partición: topic name, partition number, configured replicas, current ISR, timestamp del snapshot. Calcula métricas como el porcentaje de réplicas in-sync para cada partición.

Implementa análisis de tendencias. Si has estado ejecutando el tracker por un período prolongado, puede generar reportes que muestran cosas como: qué particiones frecuentemente caen fuera de sync, qué brokers tienen más problemas manteniéndose sincronizados, cuál es el patrón temporal de desincronizaciones (por ejemplo, si hay ciertos momentos del día donde more réplicas caen fuera de sync).

Genera alertas basadas en análisis histórico. Si una partición ha estado fuera de sincronización completa por más de un tiempo umbral (por ejemplo, cinco minutos), genera una alerta indicando que esto necesita investigación porque sincronización no debería tomar tanto tiempo en un clúster saludable.

Implementa predicción simple. Si detectas que el ISR de una partición está oscilando (réplicas entrando y saliendo del ISR repetidamente), esto indica un problema intermitente que probablemente empeorará. Alerta sobre este patrón.

Genera reportes visuales cuando se ejecuta en modo interactivo. Muestra gráficas ASCII del porcentaje de particiones in-sync en el tiempo, permitiendo ver visualmente si la salud del clúster está mejorando, estable o degradándose.

Implementa exportación de datos para herramientas de graficación externas. Genera archivos CSV que pueden importarse en Excel, Google Sheets o herramientas de BI para crear dashboards de salud del clúster.

### 13. experimentos/exp-08-replicas-basicas.md

Este es el primer experimento del Nivel Cuatro y establece los conceptos fundamentales de replicación antes de simular fallos.

**Objetivo:** Entender cómo se configuran y distribuyen las réplicas de particiones en un clúster de Kafka y qué significa el concepto de In-Sync Replicas (ISR).

**Conceptos previos necesarios:** Debes entender claramente qué es una partición, qué es un leader de partición, y cómo múltiples brokers comparten la carga de trabajo. El concepto de replicación construye sobre estos fundamentos agregando redundancia para tolerancia a fallos.

**Hipótesis:** Si creo un topic con cuatro particiones y factor de replicación tres en un clúster de tres brokers, cada partición existirá como tres copias completas, una en cada broker, y en estado saludable todas las réplicas estarán in-sync con el leader.

**Pre-requisitos:**
- Clúster de tres brokers iniciado y verificado
- Scripts del Nivel Cuatro listos para ejecutar
- ReplicaAnalyzer compilado

**Procedimiento paso a paso:**

Paso uno: Iniciar el clúster con el script veinte-iniciar-cluster.ps1 del Nivel Tres si no está ya corriendo. Verificar con el script veintiuno que los tres brokers están activos y se conocen entre sí.

Paso dos: Crear los topics replicados ejecutando el script treinta-crear-topics-replicados.ps1. Este script crea varios topics con diferentes factores de replicación. Observa cuidadosamente la salida del comando describe para cada topic creado.

Paso tres: Enfocarse en el topic transacciones-rf3 que tiene cuatro particiones con factor de replicación tres. Ejecutar el script treinta y uno-describir-replicas.ps1 para obtener un análisis detallado del estado de replicación.

Paso cuatro: Para cada partición de transacciones-rf3, identifica y documenta quién es el leader, cuáles son todas las réplicas y cuál es el ISR. Deberías ver algo como esto para cada partición:

Partition 0:
Leader: Broker 1
Replicas: [1, 2, 3]
ISR: [1, 2, 3]



Paso cinco: Verificar que efectivamente cada broker almacena todas las particiones. El Broker uno debería tener las cuatro particiones (como leader de algunas y follower de otras). Lo mismo para Brokers dos y tres. Ejecuta el ReplicaAnalyzer para confirmar esta distribución programáticamente:

```cmd
"%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-4-1.4.0.jar ^
  com.nexus.kafka.nivel4.ReplicaAnalyzer transacciones-rf3
```

Paso seis: Producir algunos mensajes al topic usando el BatchProducer del Nivel Dos. Envía cien mensajes con claves uniformemente distribuidas:

```cmd
"%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-2-1.2.0.jar ^
  com.nexus.kafka.nivel2.BatchProducer transacciones-rf3 100 true
```

Paso siete: Verificar que los mensajes se escribieron en el leader de cada partición pero también se replicaron a los followers. Puedes inferir esto porque el ISR no cambió, lo que significa que los followers se mantuvieron sincronizados incluso con nueva data llegando.

Paso ocho: Crear un topic con factor de replicación dos para comparar. Ejecuta este comando manualmente:

```powershell
docker exec kafka-broker-1 kafka-topics --create `
  --topic comparacion-rf2 `
  --partitions 4 `
  --replication-factor 2 `
  --bootstrap-server localhost:9092
```

Paso nueve: Analizar cómo se distribuyó este topic. Con replication-factor dos, cada partición existe solo en dos brokers, no en los tres. Documenta cuáles brokers tienen cada partición.

Paso diez: Comparar el almacenamiento total de datos entre los dos topics. El topic con RF tres usa más espacio de disco total porque cada mensaje se almacena tres veces. El topic con RF dos usa menos espacio pero tiene menos redundancia.

**Resultados esperados:**

Para transacciones-rf3, deberías observar que cada partición tiene exactamente tres réplicas, una en cada broker. Una de esas réplicas es el leader (la que sirve lecturas y escrituras) y las otras dos son followers (que replican datos del leader).

En un clúster saludable sin carga extrema, todas las réplicas deberían estar in-sync. Esto significa que el ISR de cada partición incluye todos los brokers en la lista de réplicas. Por ejemplo, si la Partition cero tiene réplicas en brokers uno, dos y tres, entonces su ISR debería ser uno, dos y tres.

La distribución de leaders debería estar balanceada. Kafka intenta que cada broker sea leader de aproximadamente el mismo número de particiones para distribuir la carga de trabajo equitativamente.

**Análisis:**

El factor de replicación determina cuántas copias de cada mensaje existen en el clúster. Con RF tres, puedes tolerar la pérdida simultánea de hasta dos brokers sin perder datos ni disponibilidad. Con RF dos, puedes tolerar solo un broker fallando. Con RF uno (sin replicación real), cualquier fallo de broker causa pérdida de datos.

El ISR es dinámico. Aunque configuraste tres réplicas, el ISR puede tener menos de tres réplicas si algún follower se retrasa significativamente respecto al leader. Kafka tiene umbrales configurables (replica.lag.time.max.ms) que determinan cuándo una réplica se considera fuera de sincronización.

El costo de la replicación es almacenamiento y ancho de banda de red. Con RF tres, usas tres veces más espacio de disco que sin replicación, y cada mensaje produce tráfico de red adicional cuando se replica a los followers. Este es el precio que pagas por alta disponibilidad y durabilidad.

**Conclusiones:**

Resume lo que observaste sobre cómo Kafka distribuye y mantiene réplicas. Confirma que entiendes la diferencia entre réplicas configuradas (la lista completa de brokers que deberían tener copias) y el ISR (brokers que actualmente están sincronizados). Explica por qué el ISR es crítico: solo las réplicas in-sync son elegibles para convertirse en leader si el leader actual falla.

### 14. experimentos/exp-09-fallo-leader.md

Este experimento es el corazón del Nivel Cuatro, donde finalmente simulas un fallo real de broker y observas cómo Kafka maneja el failover automáticamente.

**Objetivo:** Demostrar que cuando un broker que es leader de varias particiones falla, Kafka automáticamente elige nuevos leaders de entre los followers in-sync, permitiendo que el sistema continúe operando sin intervención manual.

**Hipótesis:** Si detengo el broker dos mientras tiene tráfico activo, las particiones donde el broker dos es leader experimentarán un failover automático a otros brokers dentro de aproximadamente cinco segundos, y el sistema continuará procesando mensajes sin pérdida de datos.

**Pre-requisitos:**
- Topic transacciones-rf3 creado y con algunas particiones lideradas por el broker dos
- FailoverMonitor compilado
- Clúster de tres brokers completamente operacional

**Procedimiento paso a paso:**

Paso uno: Antes de simular el fallo, captura el estado inicial completo del clúster ejecutando el script treinta y uno-describir-replicas.ps1. Guarda esta salida como estado de referencia.

Paso dos: Identificar cuáles particiones tienen al broker dos como leader. Ejecuta kafka-topics --describe y busca líneas donde "Leader: 2" aparece. Documenta estas particiones porque son las que experimentarán failover.

Paso tres: En una ventana de PowerShell o CMD separada, iniciar el FailoverMonitor para que capture el failover en tiempo real:

```cmd
"%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-4-1.4.0.jar ^
  com.nexus.kafka.nivel4.FailoverMonitor transacciones-rf3
```

Deja esta ventana visible para observar los eventos a medida que ocurren.

Paso cuatro: En otra ventana separada, iniciar un producer que envíe mensajes continuamente al topic. Usa el BatchProducer en un loop o modifica el DurableProducer para que envíe indefinidamente hasta que lo detengas con Ctrl+C:

```cmd
"%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-4-1.4.0.jar ^
  com.nexus.kafka.nivel4.DurableProducer transacciones-rf3 continuous all
```

Esto genera carga activa en el clúster durante el fallo.

Paso cinco: Con el monitor y el producer corriendo, ejecutar el script treinta y dos-simular-fallo-broker.ps1 especificando el broker dos:

```powershell
.\scripts\powershell\32-simular-fallo-broker.ps1 -Broker 2
```

Confirma cuando el script pregunte. El broker dos se detendrá abruptamente.

Paso seis: Inmediatamente después de ejecutar el script, observa atentamente la ventana del FailoverMonitor. Deberías ver eventos de failover aparecer en tiempo real a medida que Kafka detecta que el broker dos no responde y elige nuevos leaders.

Paso siete: Observa también la ventana del producer. Con acks=all configurado, el producer puede experimentar algunos errores temporales mientras Kafka completa el failover, pero debe recuperarse automáticamente y continuar enviando mensajes una vez que los nuevos leaders estén establecidos.

Paso ocho: Espera treinta segundos después del fallo. Ejecuta nuevamente el script treinta y uno-describir-replicas.ps1 para capturar el nuevo estado del clúster. Compara esta salida con el estado inicial del Paso uno.

Paso noveno: Verificar que todas las particiones que tenían al broker dos como leader ahora tienen un nuevo leader (debería ser el broker uno o el broker tres). Verifica que esas particiones ahora tienen solo dos réplicas in-sync en lugar de tres, porque el broker dos está caído.

Paso décimo: Detén el producer con Ctrl+C. Usa un consumer para leer todos los mensajes del topic y verificar que no hubo pérdida de mensajes durante el failover. El número de mensajes en el topic debería coincidir con el número de mensajes que el producer confirmó exitosamente.

**Resultados esperados:**

Cuando detienes el broker dos, deberías observar una secuencia de eventos en el FailoverMonitor similar a esta:


⚠️  14:52:10.234 - BROKER CAÍDO DETECTADO
• Broker 2 ya no responde
🔄 14:52:10.567 - FAILOVER EN PROGRESO
• Topic: transacciones-rf3, Partition: 0
• Leader anterior: Broker 2 (caído)
• Candidatos para nuevo leader: [1, 3]
✅ 14:52:10.789 - FAILOVER COMPLETADO
• Topic: transacciones-rf3, Partition: 0
• Nuevo leader: Broker 3
• Tiempo de failover: 555 ms


El tiempo total de failover (desde que el broker falla hasta que el nuevo leader está sirviendo tráfico) típicamente es de dos a cinco segundos en un clúster local. En producción con mejor hardware y red puede ser más rápido.

El producer con acks=all puede experimentar algunos mensajes fallidos durante la ventana de failover, pero Kafka's automatic retry mechanism debe reintentar esos mensajes y eventualmente todos deben ser confirmados exitosamente.

Después del failover, el ISR de particiones que tenían al broker dos debe mostrar solo dos brokers en lugar de tres. El broker dos sigue en la lista de réplicas configuradas pero ya no está en el ISR.

**Análisis:**

El failover automático es una de las características más valiosas de Kafka. Sin replicación, cuando un broker falla, las particiones en ese broker quedan completamente inaccesibles hasta que el broker se recupera. Con replicación y múltiples brokers in-sync, Kafka puede promover automáticamente un follower a leader en segundos.

La elección del nuevo leader no es aleatoria. Kafka solo elige de entre las réplicas que están en el ISR porque solo esas réplicas están garantizadas de tener todos los mensajes que el leader anterior había confirmado. Si Kafka eligiera una réplica fuera de sync, podrían perderse mensajes.

El tiempo de failover depende de varios factores: qué tan rápido Kafka detecta que el broker falló (configurado por timeout settings), cuánto tiempo toma la elección del nuevo leader (típicamente muy rápido), y cuánto tiempo toman los clients (producers y consumers) en descubrir el nuevo leader (requiere una actualización de metadata).

Con acks=all y min.insync.replicas configurado apropiadamente, no deberías perder ningún mensaje durante el failover. Los mensajes confirmados antes del fallo están garantizados en múltiples réplicas, y los mensajes durante la ventana de failover son reintentados automáticamente por el producer.

**Conclusiones:**

Resume lo que observaste durante el failover. Confirma que el sistema continuó operando a pesar de perder un tercio de su capacidad de almacenamiento. Explica por qué esta característica es crítica en sistemas de producción donde el uptime es fundamental.

### 15. experimentos/exp-10-durabilidad-acks.md

Este experimento explora sistemáticamente cómo diferentes configuraciones de acknowledgment en el producer afectan tanto la durabilidad de los mensajes como el rendimiento del sistema.

**Objetivo:** Medir empíricamente la diferencia en throughput y garantías de durabilidad entre acks=0, acks=1 y acks=all, especialmente bajo condiciones de fallo de brokers.

**Hipótesis:** Acks=all proporciona las mejores garantías de durabilidad a costa de menor throughput, mientras acks=0 maximiza throughput pero no proporciona garantías de durabilidad. Acks=1 es un balance intermedio.

**Pre-requisitos:**
- Topic critical-data creado con RF=3 y min.insync.replicas=2
- DurableProducer compilado
- Clúster de tres brokers operacional

**Procedimiento paso a paso:**

Paso uno: Ejecutar el DurableProducer con acks=0 para establecer la línea base de máximo throughput sin garantías de durabilidad:

```cmd
"%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-4-1.4.0.jar ^
  com.nexus.kafka.nivel4.DurableProducer critical-data 5000 0
```

Registra el throughput (mensajes por segundo) y latencia promedio reportados.

Paso dos: Ejecutar con acks=1 para ver el impacto de esperar confirmación del leader:

```cmd
"%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-4-1.4.0.jar ^
  com.nexus.kafka.nivel4.DurableProducer critical-data 5000 1
```

Registra los resultados y compara con acks=0.

Paso tres: Ejecutar con acks=all (o -1, son equivalentes) para máxima durabilidad:

```cmd
"%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-4-1.4.0.jar ^
  com.nexus.kafka.nivel4.DurableProducer critical-data 5000 all
```

Registra estos resultados. Deberías ver menor throughput pero mayor latencia comparado con acks=0 y acks=1.

Paso cuatro: Ahora vamos a probar durabilidad bajo fallo. Reinicia el clúster para limpiar el topic critical-data o crea un nuevo topic para este test. Ejecuta el DurableProducer con acks=0 en modo continuo, luego mientras está corriendo, simula fallo del broker que es leader de alguna partición. Observa qué sucede con los mensajes.

Con acks=0, el producer no espera confirmación, así que continúa enviando mensajes incluso después de que el broker falló. Esos mensajes enviados después del fallo pero antes de que el producer descubra el nuevo leader se pierden silenciosamente.

Paso cinco: Repite el test con acks=1. Ejecuta el DurableProducer con acks=1, simula fallo del leader. Con acks=1, el producer recibe errores cuando intenta enviar al broker caído, lo que dispara reintentos automáticos. Después del failover, los reintentos tienen éxito y no se pierden mensajes.

Paso seis: Repite con acks=all. Este caso es similar a acks=1 en términos de no perder mensajes, pero con acks=all tienes una garantía adicional: incluso si el leader confirma un mensaje y luego falla un milisegundo después, el mensaje está garantizado en al menos min.insync.replicas brokers.

Paso siete: Crear una tabla comparativa de todos los resultados:

╔════════════════════════════════════════════════════════╗
║        COMPARACIÓN DE NIVELES DE ACKS                  ║
╚════════════════════════════════════════════════════════╝
┌─────────┬────────────┬─────────────┬──────────────┐
│  Nivel  │ Throughput │ Latencia p95│  Durabilidad │
├─────────┼────────────┼─────────────┼──────────────┤
│ acks=0  │ 12,500 m/s │    3.2 ms   │     Baja     │
│ acks=1  │  7,800 m/s │    9.7 ms   │    Media     │
│ acks=all│  5,200 m/s │   14.3 ms   │     Alta     │
└─────────┴────────────┴─────────────┴──────────────┘
Mensajes perdidos en fallo de broker:
• acks=0: ~150 mensajes (enviados pero no confirmados)
• acks=1: 0 mensajes (errores detectados, reintentos exitosos)
• acks=all: 0 mensajes (garantizados en múltiples réplicas)


Guarda estos resultados en experimentos/resultados/comparacion-acks.txt.

**Resultados esperados:**

Deberías observar un trade-off claro entre throughput y durabilidad. Acks=0 proporciona el máximo throughput porque el producer no espera confirmación de ningún broker, simplemente envía mensajes y asume que llegaron. Acks=1 reduce el throughput aproximadamente a la mitad porque ahora el producer espera confirmación del leader. Acks=all reduce el throughput aún más porque el producer espera confirmación del leader Y todos los in-sync replicas.

La latencia sigue un patrón similar. Con acks=0, la latencia percibida por el producer es muy baja porque no está esperando nada. Con acks=1, la latencia incluye el round trip al leader. Con acks=all, la latencia incluye el tiempo que toma para que el leader replique a todos los ISR y reciba sus confirmaciones.

Bajo condiciones de fallo, acks=0 es riesgoso. Los mensajes enviados justo antes y durante el fallo pueden perderse sin que el producer lo sepa. Acks=1 y acks=all manejan fallos gracefully mediante retry automático, pero acks=all proporciona garantía adicional de que mensajes confirmados sobreviven el fallo del leader.

**Análisis:**

La elección de acks level es una de las decisiones de diseño más importantes cuando usas Kafka. No hay una respuesta "correcta" universal, depende de tus requisitos específicos.

Para datos críticos donde la pérdida de un solo mensaje es inaceptable, como transacciones financieras o eventos de auditoría, debes usar acks=all combinado con min.insync.replicas >= 2. El costo de rendimiento vale la pena para la garantía de durabilidad.

Para datos de logging o métricas donde perder ocasionalmente algunos mensajes es tolerable, acks=1 puede ser un buen balance. Obtienes mejor rendimiento que acks=all pero aún tienes detección de errores y reintentos automáticos.

Acks=0 es raramente apropiado en producción porque la pérdida silenciosa de datos es difícil de detectar y debuggear. Solo considéralo si el throughput extremo es absolutamente crítico y la pérdida de datos es verdaderamente aceptable.

Recuerda que acks=all solo es efectivo si tienes replication-factor > 1 y múltiples réplicas in-sync. Con RF=1, acks=all es equivalente a acks=1 porque no hay followers a los que replicar.

**Conclusiones:**

Resume el trade-off fundamental entre rendimiento y durabilidad que observaste. Explica cómo diferentes casos de uso justifican diferentes elecciones de acks level. Enfatiza que la configuración correcta depende de qué tan críticos son tus datos y cuánto throughput necesitas.

### 16. experimentos/exp-11-sincronizacion-replicas.md

Este experimento final del Nivel Cuatro explora el proceso de re-sincronización que ocurre cuando un broker previamente caído se recupera y sus réplicas necesitan ponerse al día con los cambios que ocurrieron durante su ausencia.

**Objetivo:** Observar y medir el proceso de re-sincronización de réplicas cuando un broker se recupera de un fallo, entendiendo cuánto tiempo toma y qué factores afectan la velocidad de sincronización.

**Hipótesis:** Cuando un broker se recupera después de estar caído, sus réplicas se re-sincronizarán automáticamente con los leaders actuales, y el tiempo de sincronización será proporcional a la cantidad de datos nuevos que se escribieron mientras el broker estuvo fuera.

**Pre-requisitos:**
- El experimento nueve completado (broker dos previamente detenido y simulado como fallido)
- Topic transacciones-rf3 con datos activos
- ISRTracker compilado

**Procedimiento paso a paso:**

Paso uno: Antes de recuperar el broker, verificar el estado actual del clúster. El broker dos debe estar detenido, y las particiones que tenían réplicas en el broker dos deben mostrar ISR reducido (solo dos de tres réplicas in-sync). Ejecuta el script treinta y cuatro-verificar-isr.ps1 y documenta el estado inicial.

Paso dos: Mientras el broker dos sigue caído, producir una cantidad significativa de mensajes nuevos al topic transacciones-rf3. Esto simula data que se escribió mientras el broker estuvo fuera. Envía al menos cinco mil mensajes:

```cmd
"%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-2-1.2.0.jar ^
  com.nexus.kafka.nivel2.BatchProducer transacciones-rf3 5000 true
```

Paso tres: Iniciar el ISRTracker en modo tracking para que capture el proceso de re-sincronización en tiempo real:

```cmd
"%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-4-1.4.0.jar ^
  com.nexus.kafka.nivel4.ISRTracker transacciones-rf3 --track --interval 5
```

Este comando toma un snapshot del estado ISR cada cinco segundos. Déjalo corriendo en una ventana separada.

Paso cuatro: Ejecutar el script treinta y tres-recuperar-broker.ps1 especificando el broker dos:

```powershell
.\scripts\powershell\33-recuperar-broker.ps1 -Broker 2
```

El script reiniciará el broker dos y comenzará a monitorear el proceso de recuperación.

Paso cinco: Observar atentamente la ventana del ISRTracker. Deberías ver las réplicas del broker dos gradualmente agregándose de vuelta al ISR de cada partición. Este proceso no es instantáneo, toma tiempo porque el broker dos necesita copiar todos los mensajes nuevos que se escribieron mientras estuvo caído.

Paso seis: Medir el tiempo total de re-sincronización. Desde que el broker dos se inició hasta que todas sus réplicas están nuevamente in-sync. El ISRTracker debe reportar este tiempo automáticamente.

Paso siete: Después de que la re-sincronización está completa, verificar que el estado del clúster volvió a normal. Todas las particiones de transacciones-rf3 deberían ahora tener tres réplicas in-sync nuevamente. Ejecuta el script treinta y uno-describir-replicas.ps1 para confirmar.

Paso ocho: Observar si Kafka rebalanceó leaders de vuelta al broker dos. En algunos casos, Kafka puede preferir mover algunos leaders de vuelta a sus posiciones "preferidas" configuradas, aunque esto depende de tu configuración de auto.leader.rebalance.enable.

Paso noveno: Repetir el experimento pero con diferente cantidad de data. Detén nuevamente el broker dos, esta vez produce solo quinientos mensajes mientras está caído, y luego recupéralo. Compara el tiempo de re-sincronización. Debería ser significativamente más rápido con menos data para sincronizar.

Paso décimo: Documentar todos los tiempos de sincronización observados y factores que afectaron la velocidad (cantidad de data, tamaño de mensajes, número de particiones afectadas).

**Resultados esperados:**

La re-sincronización debería completarse automáticamente sin intervención manual. Kafka detecta que el broker dos volvió, identifica qué réplicas están en ese broker y comienza a copiar mensajes faltantes de los leaders actuales.

El tiempo de sincronización depende principalmente de cuántos mensajes se escribieron mientras el broker estuvo caído y del throughput de red entre brokers. Con cinco mil mensajes de un kilobyte cada uno (aproximadamente cinco megabytes de data), la sincronización en un clúster local Docker podría tomar de treinta segundos a dos minutos.

Durante la re-sincronización, las réplicas del broker dos no están en el ISR, así que si tienes min.insync.replicas configurado, esas réplicas faltantes no afectan la capacidad de escribir mensajes nuevos (siempre que el ISR actual cumpla con el mínimo).

Una vez que una réplica se sincroniza completamente, Kafka automáticamente la agrega al ISR. Esto es visible en el output del ISRTracker: verás particiones pasando de ISR [1, 3] a ISR [1, 2, 3] a medida que el broker dos se sincroniza.

**Análisis:**

El proceso de re-sincronización es crítico para la auto-recuperación del clúster. Sin sincronización automática, un administrador tendría que intervenir manualmente después de cada fallo de broker, lo cual no escala en clústeres grandes.

La velocidad de sincronización está limitada por varios factores: ancho de banda de red entre brokers, velocidad de lectura del disco del broker con el leader (que está sirviendo data tanto a clients como a la réplica que se está sincronizando), y velocidad de escritura del disco del broker que se está recuperando.

Kafka prioriza consistencia sobre velocidad durante la re-sincronización. Las réplicas no se agregan al ISR hasta que están completamente sincronizadas, garantizando que cualquier réplica in-sync puede convertirse en leader sin pérdida de datos.

En un clúster de producción, querrías monitorear el tiempo de re-sincronización. Si un broker consistentemente toma mucho tiempo en re-sincronizarse, puede indicar problemas de hardware (disco lento, red lenta) o configuración sub-óptima.

El parámetro replica.lag.time.max.ms (por defecto diez segundos) determina cuánto puede retrasarse una réplica antes de ser removida del ISR. Si la re-sincronización toma más tiempo que este umbral, la réplica continuará sincronizándose pero permanecerá fuera del ISR hasta que se ponga completamente al día.

**Conclusiones:**

Resume lo que observaste sobre el proceso de auto-recuperación de Kafka. Explica cómo el sistema balancea disponibilidad (permitir lecturas/escrituras con ISR reducido) con durabilidad (solo agregar réplicas al ISR cuando están completamente sincronizadas). Enfatiza que esta auto-recuperación es fundamental para operar Kafka en producción donde fallos de hardware son inevitables.

### 17. INSTRUCCIONES-NIVEL-4.md

Documento maestro con instrucciones paso a paso para completar todo el Nivel Cuatro del laboratorio.

**Introducción:** Explica que este nivel agrega la pieza final para hacer Kafka verdaderamente robusto: replicación de particiones para tolerancia a fallos. Al completar este nivel, tendrás las habilidades para diseñar topics con el nivel apropiado de redundancia para tus necesidades de producción.

**Sección uno: Preparación del entorno**

Verificar que el Nivel Tres está completamente funcional. El clúster de tres brokers debe iniciar sin problemas. Docker Desktop debe tener suficiente RAM asignada (al menos seis gigabytes). Asegurarse de que entiendes claramente los conceptos del Nivel Tres antes de continuar.

**Sección dos: Compilación del código Java del Nivel Cuatro**

Navegar al directorio java y ejecutar Maven para compilar las nuevas clases:

```cmd
cd java
mvn clean package
```

Verificar que la compilación fue exitosa y que se generaron los JARs para el Nivel Cuatro. Recordatorio: todos los comandos java deben usar "%JAVA_HOME%\bin\java" con comillas.

**Sección tres: Creación de topics replicados**

Ejecutar el script treinta-crear-topics-replicados.ps1. Este script crea varios topics con diferentes factores de replicación. Observar cuidadosamente la salida del comando describe para cada topic, especialmente las columnas de Replicas e ISR.

Ejecutar el script treinta y uno-describir-replicas.ps1 para obtener un análisis detallado del estado inicial de replicación. Guardar esta salida como referencia.

**Sección cuatro: Experimento ocho - Réplicas básicas**

Seguir el documento exp-08-replicas-basicas.md paso a paso. Este experimento establece los conceptos fundamentales de replicación. Asegurarse de entender completamente qué significa ISR antes de continuar.

Ejecutar el ReplicaAnalyzer para obtener una vista programática del estado de replicación. Comparar su output con el del script PowerShell para confirmar que estás interpretando la información correctamente.

**Sección cinco: Experimento nueve - Fallo de leader**

Seguir exp-09-fallo-leader.md. Este es el experimento más importante del nivel. Configurar múltiples ventanas de terminal: una para el FailoverMonitor, otra para el producer continuo, otra para ejecutar el script de simular fallo.

Observar atentamente el proceso de failover en tiempo real. Tomar screenshots o capturar logs para referencia futura. Medir el tiempo de failover desde que detienes el broker hasta que el sistema está completamente operacional nuevamente.

Verificar que no se perdieron mensajes durante el failover. Usa un consumer para leer todo el topic y confirmar que el count de mensajes coincide con lo que el producer envió.

**Sección seis: Experimento diez - Durabilidad de acks**

Seguir exp-10-durabilidad-acks.md. Ejecutar el DurableProducer con cada nivel de acks y registrar los resultados. Crear una tabla comparativa mostrando throughput y latencia de cada configuración.

Simular fallos mientras produces con diferentes acks levels para ver empíricamente cómo cada configuración maneja fallos. Documentar cuántos mensajes se perdieron (si alguno) con cada configuración.

Guardar todos los resultados en experimentos/resultados/comparacion-acks.txt para referencia futura cuando necesites decidir qué acks level usar en un proyecto real.

**Sección siete: Experimento once - Sincronización de réplicas**

Seguir exp-11-sincronizacion-replicas.md. Iniciar el ISRTracker en modo tracking antes de recuperar el broker para que captures todo el proceso de re-sincronización.

Medir tiempos de sincronización con diferentes cantidades de data. Documentar cómo la cantidad de data afecta el tiempo de recuperación.

Observar cómo el sistema balancea servir tráfico de clients y sincronizar réplicas simultáneamente.

**Sección ocho: Exploración adicional**

Sugerencias de experimentos adicionales para profundizar tu entendimiento:

Primero, simular fallo de múltiples brokers simultáneamente. Con RF tres, puedes tolerar hasta dos brokers fallando. Prueba detener dos brokers y observa que el sistema sigue operando. Luego detén el tercer broker y observa que el clúster se vuelve inaccesible (porque ninguna partición tiene un leader disponible).

Segundo, experimentar con diferentes valores de min.insync.replicas. Crea topics con RF tres pero diferentes min.insync.replicas (uno, dos, tres) y observa cómo esto afecta el comportamiento bajo fallos.

Tercero, probar sincronización con volúmenes grandes de data. Produce cientos de miles de mensajes mientras un broker está caído, luego recupéralo y mide cuánto tiempo toma sincronizar esa cantidad de data.

Cuarto, explorar configuraciones de replica.lag.time.max.ms. Reduce este valor y observa cómo réplicas más fácilmente caen fuera del ISR si se retrasan levemente.

**Sección nueve: Limpieza**

Cómo detener el clúster limpiamente con el script veintitrés-detener-cluster.ps1. Si quieres resetear completamente el laboratorio, usar el script noventa y nueve-reset-laboratorio.ps1.

Para conservar el estado del clúster para continuar experimentando después, simplemente detén el clúster sin eliminar volúmenes. Los datos persistirán y cuando reinicies el clúster, todo estará como lo dejaste.

**Sección diez: Decisiones de diseño para producción**

Ahora que has completado todos los experimentos, tienes el conocimiento para tomar decisiones informadas sobre configuración de replicación en producción. Esta sección proporciona guidelines generales basadas en diferentes casos de uso.

Para datos críticos donde pérdida de cualquier mensaje es inaceptable (transacciones financieras, eventos de auditoría, comandos de control): RF igual a tres, min.insync.replicas igual a dos, acks igual a all. Esto maximiza durabilidad a costa de algún rendimiento.

Para datos importantes pero donde pérdida ocasional es tolerable (logs de aplicación, métricas de monitoreo): RF igual a dos, min.insync.replicas igual a uno, acks igual a uno. Esto balancea durabilidad y rendimiento.

Para datos de alta velocidad donde pérdida es aceptable (telemetría de sensores IoT de alta frecuencia, clicks de usuario): RF igual a dos, acks igual a uno o incluso cero si el throughput es crítico.

Para clústeres pequeños (menos de cinco brokers), usa RF igual a dos para conservar espacio de almacenamiento. Para clústeres grandes (diez o más brokers), RF igual a tres proporciona mejor tolerancia a fallos sin usar proporcionalmente más recursos.

Siempre configura min.insync.replicas al menos uno menos que RF. Con RF tres, usa min.insync.replicas dos. Esto garantiza que puedes tolerar un broker fallando sin hacer el topic read-only.

**Sección once: Próximos pasos**

Felicitaciones, has completado los cuatro niveles del laboratorio Kafka. Ahora tienes conocimiento profundo de arquitectura distribuida de Kafka, particionamiento, replicación y tolerancia a fallos.

Posibles direcciones para continuar aprendiendo:

Primero, explorar Kafka Streams para procesamiento de datos en tiempo real. Kafka Streams es una librería cliente que te permite construir aplicaciones que transforman y analizan datos almacenados en Kafka.

Segundo, aprender sobre Kafka Connect para integrar Kafka con otros sistemas. Connect proporciona conectores pre-construidos para bases de datos, sistemas de archivos, cloud storage, etcétera.

Tercero, profundizar en operaciones de Kafka: configuración avanzada de JVM, tuning de rendimiento, estrategias de backup y restore, migración de clústeres, etcétera.

Cuarto, explorar esquemas y schema registry para gestionar evolución de formato de mensajes a través del tiempo.

Quinto, aprender sobre seguridad en Kafka: autenticación, autorización, encriptación de datos en tránsito y en reposo.

---

## Validaciones importantes

Todos los scripts deben verificar prerequisites antes de ejecutar acciones potencialmente destructivas como detener brokers. Proporcionar mensajes de error claros y accionables cuando algo falla.

El código Java debe incluir comentarios explicativos abundantes, especialmente en código relacionado con replicación e ISR que son conceptos avanzados.

Los experimentos deben ser reproducibles. Cualquier persona siguiendo las instrucciones paso a paso debe poder obtener resultados similares.

---

## Formato de output

Todos los archivos con encoding UTF-8. Scripts PowerShell con line endings CRLF para Windows. Código Java indentado con cuatro espacios. Comentarios abundantes pero no excesivos. Mensajes al usuario en español. Logs técnicos pueden estar en inglés o español según sea más claro.

---

## Checklist de validación

Genera un archivo VALIDACION-NIVEL-4.md con este checklist:

- [ ] Topics replicados creados correctamente
- [ ] Entiendo qué es ISR y por qué es importante
- [ ] ReplicaAnalyzer muestra estado de réplicas correctamente
- [ ] DurableProducer ejecuta con todos los acks levels
- [ ] FailoverMonitor detecta eventos de failover
- [ ] ISRTracker rastrea cambios en ISR
- [ ] Experimento ocho completado - entiendo conceptos de réplicas
- [ ] Experimento nueve completado - observé failover automático
- [ ] Experimento diez completado - entiendo trade-off de acks
- [ ] Experimento once completado - observé re-sincronización
- [ ] Puedo simular y recuperar de fallos de brokers
- [ ] Entiendo cómo configurar topics para diferentes niveles de durabilidad
- [ ] Estoy preparado para diseñar arquitecturas Kafka en producción

---

## Pregunta final

Antes de generar todos estos archivos, hay algún aspecto de esta especificación que necesites que clarifique o alguna funcionalidad adicional que quieras incluir en el Nivel Cuatro?