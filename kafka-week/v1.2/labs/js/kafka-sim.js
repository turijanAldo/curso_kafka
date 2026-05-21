/* ============================================
   Kafka Simulator Engine
   Motor compartido para los 3 laboratorios
   ============================================ */

const KafkaSim = (() => {

  // murmur2 hash (simplified, matches Kafka's default partitioner behavior)
  function murmur2(str) {
    let h = 0x9747b28c;
    for (let i = 0; i < str.length; i++) {
      const k = str.charCodeAt(i);
      h = Math.imul(h ^ k, 0x5bd1e995);
      h ^= h >>> 13;
    }
    h ^= h >>> 15;
    return h >>> 0;
  }

  function getPartition(key, numPartitions) {
    if (key === null || key === undefined) {
      return Math.floor(Math.random() * numPartitions);
    }
    return murmur2(String(key)) % numPartitions;
  }

  // --- Topic ---
  class Topic {
    constructor(name, numPartitions) {
      this.name = name;
      this.numPartitions = numPartitions;
      this.partitions = [];
      for (let i = 0; i < numPartitions; i++) {
        this.partitions.push([]);
      }
    }

    send(key, value, headers = {}) {
      const p = getPartition(key, this.numPartitions);
      const offset = this.partitions[p].length;
      const record = {
        key,
        value,
        headers,
        partition: p,
        offset,
        timestamp: Date.now()
      };
      this.partitions[p].push(record);
      return record;
    }

    sendToPartition(partition, key, value, headers = {}) {
      const offset = this.partitions[partition].length;
      const record = { key, value, headers, partition, offset, timestamp: Date.now() };
      this.partitions[partition].push(record);
      return record;
    }

    getPartition(idx) {
      return this.partitions[idx] || [];
    }

    getAllRecords() {
      return this.partitions.flat().sort((a, b) => a.timestamp - b.timestamp);
    }

    clear() {
      this.partitions = [];
      for (let i = 0; i < this.numPartitions; i++) {
        this.partitions.push([]);
      }
    }

    getTotalMessages() {
      return this.partitions.reduce((sum, p) => sum + p.length, 0);
    }
  }

  // --- Consumer Group ---
  class ConsumerGroup {
    constructor(groupId, topic) {
      this.groupId = groupId;
      this.topic = topic;
      this.consumers = [];
      this.assignments = new Map(); // consumerId -> [partitionIndex]
      this.offsets = new Map(); // "partition" -> offset
      this.listeners = [];
    }

    addConsumer(id) {
      this.consumers.push({ id, active: true });
      this._rebalance();
      return this.consumers.length - 1;
    }

    removeConsumer(id) {
      const idx = this.consumers.findIndex(c => c.id === id);
      if (idx !== -1) {
        this.consumers.splice(idx, 1);
        this._rebalance();
      }
    }

    _rebalance() {
      const revokedMap = new Map(this.assignments);
      this.assignments.clear();

      const activeConsumers = this.consumers.filter(c => c.active);
      const numPartitions = this.topic.numPartitions;

      activeConsumers.forEach((c, i) => {
        this.assignments.set(c.id, []);
      });

      for (let p = 0; p < numPartitions; p++) {
        if (activeConsumers.length > 0) {
          const consumerIdx = p % activeConsumers.length;
          this.assignments.get(activeConsumers[consumerIdx].id).push(p);
        }
      }

      this._emit('rebalance', {
        revoked: revokedMap,
        assigned: new Map(this.assignments),
        consumers: [...this.consumers]
      });
    }

    getAssignment(consumerId) {
      return this.assignments.get(consumerId) || [];
    }

    getAllAssignments() {
      return new Map(this.assignments);
    }

    getActiveCount() {
      return this.consumers.filter(c => c.active).length;
    }

    getIdleConsumers() {
      return this.consumers.filter(c => {
        const parts = this.assignments.get(c.id);
        return !parts || parts.length === 0;
      });
    }

    onRebalance(fn) {
      this.listeners.push(fn);
    }

    _emit(event, data) {
      this.listeners.forEach(fn => fn(event, data));
    }
  }

  // --- Pedido Model ---
  const ESTADOS = ['CREADO', 'PAGADO', 'ENVIADO', 'ENTREGADO'];

  function createPedido(orderId, clienteId, producto, total) {
    return { order_id: orderId, clienteId, producto, total, estado: null, timestamp: Date.now() };
  }

  function getEstadoClass(estado) {
    if (!estado) return 'ninguno';
    return estado.toLowerCase();
  }

  // --- DLT Logic ---
  function shouldSendToDLT(pedido, threshold = 200) {
    return pedido.total > threshold;
  }

  function createDLTHeaders(record, errorMessage) {
    return {
      'error-message': errorMessage,
      'original-topic': record.topic || 'pedidos-estados',
      'original-partition': String(record.partition),
      'original-offset': String(record.offset),
      'failed-at-ms': String(Date.now())
    };
  }

  // --- Utility ---
  function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  function formatTimestamp(ts) {
    const d = new Date(ts);
    return d.toLocaleTimeString('es-MX', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' });
  }

  return {
    murmur2,
    getPartition,
    Topic,
    ConsumerGroup,
    ESTADOS,
    createPedido,
    getEstadoClass,
    shouldSendToDLT,
    createDLTHeaders,
    delay,
    formatTimestamp
  };

})();

if (typeof module !== 'undefined') module.exports = KafkaSim;
