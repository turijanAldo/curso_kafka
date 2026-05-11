# Checklist de Validación - Laboratorio Kafka Nivel 1

Marca cada item a medida que lo completas y validas.

---

## Infraestructura y entorno

- [ ] Estructura de directorios creada correctamente (ver árbol en README.md)
- [ ] Docker Desktop está instalado y corriendo
- [ ] Puerto 9092 estaba disponible antes de iniciar Kafka
- [ ] Script `00-verificar-requisitos` ejecutó sin errores

## Docker y Kafka

- [ ] `docker-compose up -d` levanta el contenedor sin errores
- [ ] Contenedor `kafka-nivel1` aparece en `docker ps` con estado `healthy`
- [ ] Puerto 9092 responde desde el host Windows (`Test-NetConnection localhost 9092`)
- [ ] Logs del contenedor no muestran errores críticos (`docker logs kafka-nivel1`)

## Scripts PowerShell

- [ ] `00-verificar-requisitos.ps1` se ejecuta sin errores de PowerShell
- [ ] `01-iniciar-kafka.ps1` levanta el clúster y muestra "KAFKA INICIADO CORRECTAMENTE"
- [ ] `02-verificar-cluster.ps1` muestra información del broker y estado KRaft
- [ ] `03-detener-kafka.ps1` detiene el clúster correctamente
- [ ] `04-limpiar-todo.ps1` pide confirmación y limpia el entorno

## Scripts CMD

- [ ] `00-verificar-requisitos.bat` se ejecuta en CMD sin errores
- [ ] `01-iniciar-kafka.bat` levanta el clúster desde CMD
- [ ] `02-verificar-cluster.bat` muestra información del clúster
- [ ] `03-detener-kafka.bat` detiene el clúster desde CMD
- [ ] `04-limpiar-todo.bat` limpia el entorno desde CMD

## Código Java

- [ ] `mvn clean package` compila sin errores
- [ ] Archivo `target/kafka-lab-nivel-1-1.0.0.jar` fue generado
- [ ] `SimpleProducer.java` envía mensajes con confirmación de offset
- [ ] `SimpleConsumer.java` recibe mensajes con metadatos completos

## Experimento 01 — Primer Mensaje

- [ ] Topic `primer-topic` creado exitosamente con 1 partición
- [ ] Topic aparece en `kafka-topics.sh --list`
- [ ] Consumer inició y mostró "🔵 Consumer iniciado"
- [ ] Producer envió mensaje y mostró "✅ Mensaje enviado exitosamente"
- [ ] Consumer recibió el mensaje y mostró "📨 Mensaje recibido" con offset correcto
- [ ] Offset del producer coincide con offset del consumer (ambos en 0 para el primer mensaje)

## Persistencia y reinicio

- [ ] Al ejecutar `03-detener-kafka.ps1`, el clúster se detuvo
- [ ] Al ejecutar `01-iniciar-kafka.ps1` de nuevo, el clúster reinició
- [ ] El topic `primer-topic` y sus mensajes siguen existentes tras el reinicio
- [ ] El consumer con `auto.offset.reset=earliest` recibió el mensaje histórico

## Comprensión conceptual

- [ ] Entiendo qué es un topic y para qué sirve
- [ ] Entiendo qué es una partición y un offset
- [ ] Entiendo por qué el producer necesita `acks=all`
- [ ] Entiendo qué hace `auto.offset.reset=earliest`
- [ ] Entiendo qué es un consumer group y para qué sirve
- [ ] Entiendo por qué KRaft reemplaza a ZooKeeper

---

## Notas personales

*(Espacio libre para observaciones, dudas o hallazgos durante el laboratorio)*

```

```

---

**Nivel 1 completado el:** _______________

**Tiempo total invertido:** _______________

**Listo para el Nivel 2:** [ ] Sí / [ ] Necesito repasar: _______________
