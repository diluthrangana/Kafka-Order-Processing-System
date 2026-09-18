/**
 * Kafka Order Processing System - Modern UI Interactive Dashboard
 */

document.addEventListener('DOMContentLoaded', () => {
  // --- State Variables ---
  let isSimulating = true; // Auto-start live stream on page load!
  let simTimer = null;
  let orderIdCounter = 1001;
  let totalProcessedCount = 0;
  let overallTotalSum = 0.0;
  let totalRetriesCount = 0;

  const productStats = {
    Item1: { count: 0, total: 0.0 },
    Item2: { count: 0, total: 0.0 },
    Item3: { count: 0, total: 0.0 },
    Item4: { count: 0, total: 0.0 },
    Item5: { count: 0, total: 0.0 }
  };

  const dlqRecords = [];
  let currentOffset = 0;

  // --- DOM Elements ---
  const toggleSimBtn = document.getElementById('toggle-sim-btn');
  const simIcon = document.getElementById('sim-icon');
  const simBtnText = document.getElementById('sim-btn-text');
  const sendSingleBtn = document.getElementById('send-single-btn');
  const sendBadBtn = document.getElementById('send-bad-btn');
  const clearLogsBtn = document.getElementById('clear-logs-btn');

  const failRateSlider = document.getElementById('fail-rate-slider');
  const failRateVal = document.getElementById('fail-rate-val');
  const invalidRateSlider = document.getElementById('invalid-rate-slider');
  const invalidRateVal = document.getElementById('invalid-rate-val');
  const speedSlider = document.getElementById('speed-slider');
  const speedVal = document.getElementById('speed-val');

  const metricOverallAvg = document.getElementById('metric-overall-avg');
  const metricProcessedCount = document.getElementById('metric-processed-count');
  const metricTotalOrders = document.getElementById('metric-total-orders');
  const metricRetries = document.getElementById('metric-retries');
  const metricDlqCount = document.getElementById('metric-dlq-count');

  const productStatsContainer = document.getElementById('product-stats-container');
  const logStreamContainer = document.getElementById('log-stream-container');
  const dlqTableBody = document.getElementById('dlq-table-body');
  const dlqTypeFilter = document.getElementById('dlq-type-filter');

  // Modal Elements
  const payloadModal = document.getElementById('payload-modal');
  const closeModalBtn = document.getElementById('close-modal');
  const modalErrType = document.getElementById('modal-err-type');
  const modalKey = document.getElementById('modal-key');
  const modalRetries = document.getElementById('modal-retries');
  const modalTime = document.getElementById('modal-time');
  const modalHeaders = document.getElementById('modal-headers');
  const modalPayload = document.getElementById('modal-payload');

  // --- Initial Render & Auto-Start ---
  renderProductCards();
  appendLog('SYSTEM', 'Kafka Consumer active on topic "orders" | Auto-Commit: Disabled');

  // Auto-start live stream right away on load
  updateSimButtonUI();
  produceAndProcessOrder();
  restartSimulationTimer();

  // --- Event Listeners ---
  if (failRateSlider) {
    failRateSlider.addEventListener('input', (e) => {
      failRateVal.textContent = `${e.target.value}%`;
    });
  }

  if (invalidRateSlider) {
    invalidRateSlider.addEventListener('input', (e) => {
      invalidRateVal.textContent = `${e.target.value}%`;
    });
  }

  if (speedSlider) {
    speedSlider.addEventListener('input', (e) => {
      speedVal.textContent = `${(e.target.value / 1000).toFixed(1)}s`;
      if (isSimulating) {
        restartSimulationTimer();
      }
    });
  }

  if (toggleSimBtn) {
    toggleSimBtn.addEventListener('click', () => {
      isSimulating = !isSimulating;
      updateSimButtonUI();
      if (isSimulating) {
        produceAndProcessOrder();
        restartSimulationTimer();
        appendLog('SYSTEM', 'Auto-Producer stream resumed.');
      } else {
        stopSimulationTimer();
        appendLog('SYSTEM', 'Auto-Producer stream paused.');
      }
    });
  }

  function updateSimButtonUI() {
    if (!toggleSimBtn || !simIcon || !simBtnText) return;
    if (isSimulating) {
      simIcon.className = 'fa-solid fa-pause';
      simBtnText.textContent = 'Pause Producer';
      toggleSimBtn.classList.remove('btn-primary');
      toggleSimBtn.classList.add('btn-secondary');
    } else {
      simIcon.className = 'fa-solid fa-play';
      simBtnText.textContent = 'Start Producer';
      toggleSimBtn.classList.remove('btn-secondary');
      toggleSimBtn.classList.add('btn-primary');
    }
  }

  if (sendSingleBtn) {
    sendSingleBtn.addEventListener('click', () => {
      produceAndProcessOrder();
    });
  }

  if (sendBadBtn) {
    sendBadBtn.addEventListener('click', () => {
      produceBadBytesMessage();
    });
  }

  if (clearLogsBtn) {
    clearLogsBtn.addEventListener('click', () => {
      logStreamContainer.innerHTML = '';
      appendLog('SYSTEM', 'Log stream cleared by user.');
    });
  }

  if (dlqTypeFilter) {
    dlqTypeFilter.addEventListener('change', () => {
      renderDlqTable();
    });
  }

  if (closeModalBtn) {
    closeModalBtn.addEventListener('click', () => {
      payloadModal.style.display = 'none';
    });
  }

  window.addEventListener('click', (e) => {
    if (e.target === payloadModal) {
      payloadModal.style.display = 'none';
    }
  });

  // --- Simulation Engine ---
  function restartSimulationTimer() {
    stopSimulationTimer();
    const intervalMs = parseInt(speedSlider ? speedSlider.value : 1500, 10);
    simTimer = setInterval(() => {
      if (isSimulating) {
        produceAndProcessOrder();
      }
    }, intervalMs);
  }

  function stopSimulationTimer() {
    if (simTimer) {
      clearInterval(simTimer);
      simTimer = null;
    }
  }

  const productsList = ['Item1', 'Item2', 'Item3', 'Item4', 'Item5'];

  function produceAndProcessOrder() {
    const invalidRate = parseInt(invalidRateSlider ? invalidRateSlider.value : 15, 10) / 100;
    const isInvalid = Math.random() < invalidRate;
    const orderId = `${orderIdCounter++}`;
    const product = productsList[Math.floor(Math.random() * productsList.length)];
    let price = +(Math.random() * 490 + 10).toFixed(2);

    if (isInvalid) {
      price = -price; // Negative price -> Validation error
    }

    const order = { orderId, product, price };
    currentOffset++;

    appendLog('PRODUCER', `Produced order key=${orderId} | ${product} | price=$${price.toFixed(2)} -> topic 'orders'`);

    processMessageWithConsumer(order, currentOffset);
  }

  function produceBadBytesMessage() {
    currentOffset++;
    const key = `BAD_${Math.floor(Math.random() * 900 + 100)}`;
    appendLog('PRODUCER', `Produced corrupted raw non-Avro byte payload [key=${key}] -> topic 'orders'`);
    
    setTimeout(() => {
      const reason = 'AvroDeserializer exception: corrupt schema magic byte';
      appendLog('DLQ', `key=${key} sent to 'orders-dlq' | type=DESERIALIZATION_ERROR | reason=${reason}`);
      
      addDlqRecord({
        key: key,
        type: 'DESERIALIZATION_ERROR',
        reason: reason,
        retries: 0,
        offset: currentOffset,
        payload: '<raw corrupted byte array: 0x89 0x4f 0x52 0x44>'
      });
    }, 200);
  }

  function processMessageWithConsumer(order, offset) {
    // 1. Validation Check
    if (order.price <= 0) {
      setTimeout(() => {
        const reason = `Invalid price: ${order.price}`;
        appendLog('DLQ', `key=${order.orderId} sent to 'orders-dlq' | type=VALIDATION_ERROR | reason=${reason}`);
        addDlqRecord({
          key: order.orderId,
          type: 'VALIDATION_ERROR',
          reason: reason,
          retries: 0,
          offset: offset,
          payload: order
        });
      }, 200);
      return;
    }

    // 2. Process with Simulated Temporary Failures & Retry
    const failRate = parseInt(failRateSlider ? failRateSlider.value : 30, 10) / 100;
    let attempts = 0;
    const maxRetries = 3;

    function attemptProcess() {
      if (Math.random() < failRate) {
        attempts++;
        totalRetriesCount++;
        if (metricRetries) metricRetries.textContent = totalRetriesCount;

        if (attempts <= maxRetries) {
          const backoffSec = Math.pow(2, attempts - 1);
          appendLog('RETRY', `order=${order.orderId} retry ${attempts}/${maxRetries} in ${backoffSec}s (Simulated downstream timeout)`);
          setTimeout(attemptProcess, 400); // Visual backoff
        } else {
          // Retries Exhausted -> Send to DLQ
          const reason = `Gave up after ${maxRetries} retries`;
          appendLog('DLQ', `key=${order.orderId} sent to 'orders-dlq' | type=RETRIES_EXHAUSTED | reason=${reason}`);
          addDlqRecord({
            key: order.orderId,
            type: 'RETRIES_EXHAUSTED',
            reason: reason,
            retries: maxRetries,
            offset: offset,
            payload: order
          });
        }
      } else {
        // Success
        totalProcessedCount++;
        overallTotalSum += order.price;
        
        productStats[order.product].count++;
        productStats[order.product].total += order.price;

        updateMetrics();
        renderProductCards();

        const retryNote = attempts > 0 ? ` (after ${attempts} retries)` : '';
        const overallAvg = (overallTotalSum / totalProcessedCount).toFixed(2);
        const prodAvg = (productStats[order.product].total / productStats[order.product].count).toFixed(2);

        appendLog('OK', `order=${order.orderId} ${order.product} price=$${order.price.toFixed(2)}${retryNote} | RUNNING AVG=$${overallAvg} (n=${totalProcessedCount}) | ${order.product} avg=$${prodAvg}`);
      }
    }

    setTimeout(attemptProcess, 150);
  }

  // --- UI Update Helpers ---
  function updateMetrics() {
    const avg = totalProcessedCount > 0 ? (overallTotalSum / totalProcessedCount).toFixed(2) : '0.00';
    if (metricOverallAvg) metricOverallAvg.textContent = avg;
    if (metricProcessedCount) metricProcessedCount.textContent = totalProcessedCount;
    if (metricTotalOrders) metricTotalOrders.textContent = totalProcessedCount;
  }

  function renderProductCards() {
    if (!productStatsContainer) return;
    productStatsContainer.innerHTML = '';
    productsList.forEach(prod => {
      const data = productStats[prod];
      const avg = data.count > 0 ? (data.total / data.count).toFixed(2) : '0.00';
      
      const card = document.createElement('div');
      card.className = 'product-stat-card';
      card.innerHTML = `
        <span class="product-name">${prod}</span>
        <span class="product-avg">$${avg}</span>
        <span class="product-count">${data.count} orders</span>
      `;
      productStatsContainer.appendChild(card);
    });
  }

  function appendLog(type, message) {
    if (!logStreamContainer) return;
    const entry = document.createElement('div');
    entry.className = `log-entry log-${type.toLowerCase()}`;
    
    const timeStr = new Date().toLocaleTimeString();
    entry.innerHTML = `<span class="log-time">[${timeStr}]</span> <span class="log-tag">[${type}]</span> <span class="log-msg">${message}</span>`;
    
    logStreamContainer.appendChild(entry);
    logStreamContainer.scrollTop = logStreamContainer.scrollHeight;
  }

  function addDlqRecord(record) {
    record.timestamp = new Date().toISOString();
    dlqRecords.unshift(record);
    if (metricDlqCount) metricDlqCount.textContent = dlqRecords.length;
    renderDlqTable();
  }

  function renderDlqTable() {
    if (!dlqTableBody) return;
    const filter = dlqTypeFilter ? dlqTypeFilter.value : 'ALL';
    const filtered = dlqRecords.filter(r => filter === 'ALL' || r.type === filter);

    if (filtered.length === 0) {
      dlqTableBody.innerHTML = `
        <tr class="empty-row">
          <td colspan="6"><i class="fa-solid fa-circle-check"></i> No failed messages in DLQ matching filter</td>
        </tr>
      `;
      return;
    }

    dlqTableBody.innerHTML = '';
    filtered.forEach((rec, idx) => {
      const tr = document.createElement('tr');
      
      let badgeClass = 'badge-validation';
      if (rec.type === 'DESERIALIZATION_ERROR') badgeClass = 'badge-deserialization';
      if (rec.type === 'RETRIES_EXHAUSTED') badgeClass = 'badge-exhausted';

      tr.innerHTML = `
        <td><strong>${rec.key}</strong></td>
        <td><span class="badge-err ${badgeClass}">${rec.type}</span></td>
        <td>${rec.reason}</td>
        <td>${rec.retries}</td>
        <td>#${rec.offset}</td>
        <td><button class="btn btn-xs btn-outline inspect-btn" data-index="${idx}"><i class="fa-solid fa-eye"></i> Inspect</button></td>
      `;
      dlqTableBody.appendChild(tr);
    });

    document.querySelectorAll('.inspect-btn').forEach(btn => {
      btn.addEventListener('click', (e) => {
        const index = parseInt(e.currentTarget.getAttribute('data-index'), 10);
        openModal(filtered[index]);
      });
    });
  }

  function openModal(record) {
    if (!payloadModal) return;
    modalErrType.textContent = record.type;
    modalErrType.className = `badge ${record.type === 'DESERIALIZATION_ERROR' ? 'badge-deserialization' : 'badge-validation'}`;
    modalKey.textContent = record.key;
    modalRetries.textContent = record.retries;
    modalTime.textContent = record.timestamp;

    const headers = {
      "dlq.error.type": record.type,
      "dlq.error.reason": record.reason,
      "dlq.original.topic": "orders",
      "dlq.original.partition": "0",
      "dlq.original.offset": `${record.offset}`,
      "dlq.retry.count": `${record.retries}`,
      "dlq.failed.at": record.timestamp
    };

    modalHeaders.textContent = JSON.stringify(headers, null, 2);
    modalPayload.textContent = typeof record.payload === 'object' ? JSON.stringify(record.payload, null, 2) : record.payload;

    payloadModal.style.display = 'flex';
  }
});
