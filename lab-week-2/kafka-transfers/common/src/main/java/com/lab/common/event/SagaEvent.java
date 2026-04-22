package com.lab.common.event;

/**
 * Contrato mínimo que cumple cualquier evento del Saga.
 *
 * Al implementar esta interfaz, todos los eventos del Saga tienen
 * un contrato unificado que permite tratarlos de forma polimórfica
 * sin conocer el tipo concreto.
 *
 * USO PRINCIPAL — correlationId en MDC:
 *
 *   // En cualquier consumer que recibe Object:
 *   if (event instanceof SagaEvent se) {
 *       MDC.put("txId", se.getTransactionId());
 *   }
 *
 * Sin esta interfaz, habría que encadenar instanceof para cada tipo
 *   solo para extraer el transactionId — duplicación de lógica.
 *
 * ¿Por qué solo transactionId y fromAccount, y no amount o timestamp?
 * MDC es para logging y tracing — no para lógica de negocio.
 * Con transactionId podemos correlacionar logs entre servicios.
 * Con fromAccount podemos filtrar por cuenta en sistemas de logging.
 * El resto de campos (amount, timestamp) pertenecen al negocio, no al trace.
 *
 * Implementaciones:
 *   TransferRequestedEvent, TransferValidatedEvent, TransferFailedEvent,
 *   TransferDebitedEvent, TransferCreditedEvent, TransferCompensatedEvent
 */
public interface SagaEvent {

    /** ID único de la transferencia — correlaciona todos los eventos del mismo Saga */
    String getTransactionId();

    /** Cuenta origen — útil para filtrar logs por cliente */
    String getFromAccount();
}
