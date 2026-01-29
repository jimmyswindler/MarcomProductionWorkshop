
// State
let currentShipment = { ship_to: {}, orders: [], all_expected_barcodes: [], scanned_barcodes: new Set(), boxWeights: {}, orderProgress: {} };
let packageList = [];
let appMode = 'SCANNING_BOXES';

// Elements
const el = (id) => document.getElementById(id);
const step1 = el('step1-scan-order');
const step2 = el('step2-scan-boxes');
const step4 = el('step4-scan-carton');
const orderInput = el('order-id-input');
const boxInput = el('box-barcode-input');
const cartonInput = el('carton-input');
const multiModeCheckbox = el('multi-mode-checkbox');

// Status Helper
function showStatus(element, message, type = 'info', autoHide = true) {
    if (!element) return;
    element.textContent = message;
    element.style.display = 'block';
    element.className = `status status-${type}`;
    if (autoHide) setTimeout(() => { element.style.display = 'none'; }, 5000);
}

function updateButtonState(btn, enabled, activeClass = 'active-blue') {
    if (!btn) return;
    btn.disabled = !enabled;
    if (enabled) {
        btn.classList.add(activeClass);
    } else {
        btn.classList.remove(activeClass);
        // Also remove other potential active classes to be safe
        btn.classList.remove('active-success', 'active-info', 'active-danger');
    }
}

function resetAll() {
    window.location.reload();
}

// Initialization
window.onload = function () {
    initBarcodes();
    initListeners();
    if (orderInput) orderInput.focus();
};

function initBarcodes() {
    const cmds = [
        { id: "#bc-process", val: "CMD-PROCESS" },
        { id: "#bc-add", val: "CMD-ADD-CL" },
        { id: "#bc-cancel", val: "CMD-CANCEL-ORDER" },
        { id: "#bc-finish", val: "CMD-FINISH-SHIP" },
        { id: "#bc-multi", val: "CMD-TOGGLE-MULTI" },
        { id: "#bc-custom", val: "CMD-TOGGLE-CUSTOM" }
    ];
    cmds.forEach(c => {
        try { JsBarcode(c.id, c.val, { format: "CODE128", width: 2, height: 40, displayValue: false, margin: 0 }); }
        catch (e) { }
    });
    document.querySelectorAll('.bc-render').forEach(el => {
        JsBarcode(el, el.dataset.value, { format: "CODE128", width: 2, height: 40, displayValue: false });
    });
}

function initListeners() {
    // 1. Order Entry
    orderInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' && orderInput.value.trim()) {
            fetchOrderData(orderInput.value.trim());
        }
    });

    // 2. Global Scan Listener
    let scanBuffer = "";
    let scanTimeout;
    window.addEventListener('keydown', (e) => {
        if (e.ctrlKey || e.altKey || e.metaKey) return;
        if (e.key === 'Enter') {
            if (scanBuffer.length > 2) handleGlobalScan(scanBuffer);
            scanBuffer = "";
            return;
        }
        if (e.key.length === 1) {
            scanBuffer += e.key;
            clearTimeout(scanTimeout);
            scanTimeout = setTimeout(() => { scanBuffer = ""; }, 100);
        }
    });

    // 3. Step 2 Box Input
    boxInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' && boxInput.value.trim()) {
            e.stopPropagation();
            const val = boxInput.value.trim();
            // Check if it matches a Command or Tracking #
            if (val.startsWith('1Z')) {
                showStatus(el('box-scan-status'), 'Tracking number ignored.', 'warn');
                boxInput.value = ''; return;
            }
            if (val === 'CMD-PROCESS') return; // Handled by global

            if (appMode === 'SCANNING_BOXES') processBoxScan(val);
            // else fetchAndCompareOrder(val); // Removed comparison logic for simplicity in refactor, can re-add if needed
        }
    });

    // 4. Buttons - Step 2
    el('process-shipment-btn').addEventListener('click', () => {
        if (!el('process-shipment-btn').disabled) goToPackStep();
    });
    el('cancel-btn').addEventListener('click', resetAll);
    el('add-order-btn').addEventListener('click', () => {
        alert("Add Order feature coming soon");
    });

    // 5. Buttons - Step 4
    el('finish-shipment-btn').addEventListener('click', finalizeShipment);
    el('toggle-multi-btn').addEventListener('click', () => handleGlobalScan('CMD-TOGGLE-MULTI'));
    el('toggle-custom-btn').addEventListener('click', () => handleGlobalScan('CMD-TOGGLE-CUSTOM'));

    if (el('clear-cartons-btn')) {
        el('clear-cartons-btn').addEventListener('click', () => {
            packageList = [];
            renderPackedList();
            document.querySelectorAll('.box-btn').forEach(b => b.classList.remove('selected'));
        });
    }

    // Box Buttons
    document.querySelectorAll('.box-btn').forEach(btn => {
        const boxId = btn.dataset.box;
        // Inject Weight Display Span
        let wSpan = document.getElementById(`w-display-${boxId.replace('#', '')}`);
        if (!wSpan) {
            wSpan = document.createElement('span');
            wSpan.id = `w-display-${boxId.replace('#', '')}`;
            wSpan.style.fontWeight = 'bold';
            wSpan.style.marginLeft = '10px';
            wSpan.style.fontSize = '1.2em';
            btn.appendChild(wSpan);
        }
        btn.addEventListener('click', () => handleCartonInput(boxId));
    });

    // Custom Box Add
    if (el('add-custom-btn')) {
        el('add-custom-btn').addEventListener('click', addCustomCarton);
    }
}

function handleGlobalScan(code) {
    code = code.trim();
    if (code === 'CMD-CANCEL-ORDER') return resetAll();

    // Step 2 Commands
    if (step2.style.display !== 'none') {
        if (code === 'CMD-PROCESS') {
            if (!el('process-shipment-btn').disabled) goToPackStep();
            return;
        }
    }

    // Step 4 Commands
    if (step4.style.display !== 'none') {
        if (code === 'CMD-FINISH-SHIP') {
            if (!el('finish-shipment-btn').disabled) finalizeShipment();
            return;
        }
        if (code === 'CMD-TOGGLE-MULTI') {
            multiModeCheckbox.click();
            el('multi-mode-text').textContent = multiModeCheckbox.checked ? "Disable Multi-Carton Mode" : "Enable Multi-Carton Mode";
            el('toggle-multi-btn').classList.toggle('active-info', multiModeCheckbox.checked);

            // Clear Selection
            document.querySelectorAll('[id^="w-display-"]').forEach(s => s.textContent = '');
            packageList = [];
            document.querySelectorAll('.box-btn').forEach(b => b.classList.remove('selected'));
            renderPackedList();
            showStatus(el('carton-status'), "Selection cleared due to mode switch.", 'info');
            return;
        }
        if (code === 'CMD-TOGGLE-CUSTOM') {
            const customSec = el('custom-box-section');
            const isVisible = customSec.style.display !== 'none';
            customSec.style.display = isVisible ? 'none' : 'block';
            el('custom-mode-text').textContent = !isVisible ? "Use Standard Box" : "Use Custom Box";
            return;
        }
    }
}

async function fetchOrderData(id) {
    showStatus(el('status-message'), 'Loading...', 'warn', false);
    try {
        const res = await fetch(`/api/order/${id}`);
        if (!res.ok) throw await res.json();
        const data = await res.json();

        currentShipment = {
            ship_to: data.ship_to,
            orders: [data],
            all_expected_barcodes: [...(data.expected_barcodes || [])],
            scanned_barcodes: new Set(),
            boxWeights: {},
            orderProgress: data.order_progress || {} // Ensure object
        };

        // Populate weights map
        if (data.line_items) {
            data.line_items.forEach(li => {
                li.barcodes.forEach(bc => currentShipment.boxWeights[bc.value] = bc.estimated_weight || 1.0);
            });
        }

        setupStep2();

    } catch (e) {
        showStatus(el('status-message'), e.error || 'Error', 'error');
        orderInput.value = '';
    }
}

function setupStep2() {
    step1.style.display = 'none';
    step2.style.display = 'block';
    el('last-shipment-display').style.display = 'none';

    const orderNum = currentShipment.orders[0].related_order_number || currentShipment.orders[0].order_number;
    el('scanning-header').textContent = `Scanning Items for ${orderNum}`;

    const a = currentShipment.ship_to;
    el('shipping-address').innerHTML = `<strong>Ship To:</strong> ${a.name} (Store #: ${a.store_number || 'N/A'})<br>${a.address1}<br>${a.city}, ${a.state} ${a.zip}`;

    updateBarcodeList();
    boxInput.value = ''; boxInput.disabled = false; boxInput.focus();

    updateButtonState(el('process-shipment-btn'), false);
    updateButtonState(el('add-order-btn'), false);
}

function updateBarcodeList() {
    // 1. Order Progress Bar
    // Simplified progress for now, assuming 0/0 if missing
    el('scan-progress-bar').style.width = `0%`;
    el('scan-progress-text').textContent = `0 / 0`;

    // 2. Render Job Groups
    el('expected-barcodes').innerHTML = ''; // Clear List
    const allItems = currentShipment.orders.flatMap(o => o.line_items || []);

    const grouped = {};
    allItems.forEach(i => {
        const k = i.job_ticket;
        if (!grouped[k]) grouped[k] = [];
        grouped[k].push(i);
    });

    Object.keys(grouped).sort().forEach(jt => {
        const items = grouped[jt];
        // Calculate Job Progress
        let jobTotal = 0;
        let jobPacked = 0;
        items.forEach(i => i.barcodes.forEach(b => {
            jobTotal++;
            if (b.status === 'packed' || currentShipment.scanned_barcodes.has(b.value)) jobPacked++;
        }));

        // Job Container
        const jDiv = document.createElement('div');
        jDiv.style.border = "1px solid #ddd";
        jDiv.style.marginBottom = "20px";
        jDiv.style.borderRadius = "5px";

        // Job Header
        const jHead = document.createElement('div');
        jHead.style.padding = "10px";
        jHead.style.background = "#f8f9fa";
        jHead.style.display = "flex";
        jHead.style.justifyContent = "space-between";
        jHead.innerHTML = `<strong>${jt}</strong> <span>${jobPacked}/${jobTotal}</span>`;

        // Mini Job Progress Bar
        const jpBg = document.createElement('div');
        jpBg.style.height = "5px"; jpBg.style.background = "#eee";
        const jpFill = document.createElement('div');
        jpFill.style.height = "100%"; jpFill.style.background = "#28a745";
        jpFill.style.width = jobTotal > 0 ? `${(jobPacked / jobTotal) * 100}%` : '0%';
        jpBg.appendChild(jpFill);

        jDiv.appendChild(jHead);
        jDiv.appendChild(jpBg);

        // Items
        const iList = document.createElement('div');
        iList.style.padding = "10px";
        items.forEach(item => {
            const iRow = document.createElement('div');
            iRow.style.marginBottom = "10px";
            iRow.innerHTML = `<div>${item.sku} <small>${item.sku_description || ''}</small></div>`;

            const bcRow = document.createElement('div');
            bcRow.className = 'barcode-grid';
            bcRow.style.display = "flex";
            bcRow.style.gap = "10px";
            bcRow.style.flexWrap = "wrap";

            item.barcodes.forEach(bc => {
                const isScanned = currentShipment.scanned_barcodes.has(bc.value);
                const isPacked = bc.status === 'packed';
                let style = "border: 1px solid #ccc; background: white;";
                if (isPacked) style = "background:#e2e6ea; border-color:#ccc; color:#777;";
                if (isScanned) style = "background:#d4edda; border-color:#28a745;";

                bcRow.innerHTML += `<div class="barcode-card" style="min-width:120px; padding:5px; text-align:center; border-radius:4px; ${style}">
                    <div style="font-weight:bold;">${bc.value}</div>
                    <small>${isPacked ? 'PACKED' : ''}</small>
                 </div>`;
            });
            iRow.appendChild(bcRow);
            iList.appendChild(iRow);
        });

        jDiv.appendChild(iList);
        el('expected-barcodes').appendChild(jDiv);
    });
}

function processBoxScan(code) {
    if (!currentShipment.all_expected_barcodes.includes(code)) {
        return showStatus(el('box-scan-status'), `Invalid: ${code}`, 'error'), boxInput.value = '';
    }
    if (currentShipment.scanned_barcodes.has(code)) {
        return showStatus(el('box-scan-status'), `Dup: ${code}`, 'warn'), boxInput.value = '';
    }

    // Check if "already packed" (server side status)
    // Simplified check: we trust the initial data.
    // If logic needed, iterate orders -> line_items -> barcodes

    currentShipment.scanned_barcodes.add(code);
    updateBarcodeList();
    showStatus(el('box-scan-status'), `OK: ${code}`, 'success');

    checkProcessShipmentEligibility();

    if (currentShipment.scanned_barcodes.size === currentShipment.all_expected_barcodes.length) {
        boxInput.disabled = true;
        el('process-shipment-btn').focus();
    }
    boxInput.value = '';
}

function checkProcessShipmentEligibility() {
    let anyLineComplete = false;
    let anyLinePartial = false;

    if (!currentShipment || !currentShipment.orders) return;

    currentShipment.orders.forEach(o => (o.line_items || []).forEach(li => {
        const totalBoxes = li.barcodes.length;
        let scannedCount = 0;
        li.barcodes.forEach(b => {
            if (b.status === 'packed' || currentShipment.scanned_barcodes.has(b.value)) {
                scannedCount++;
            }
        });
        if (totalBoxes > 0) {
            if (scannedCount === totalBoxes) anyLineComplete = true;
            else if (scannedCount > 0) anyLinePartial = true;
        }
    }));

    const canProcess = anyLineComplete && !anyLinePartial;
    updateButtonState(el('process-shipment-btn'), canProcess, 'active-blue');
}

function goToPackStep() {
    step2.style.display = 'none'; step4.style.display = 'block';

    // Calculate Weight
    let totalW = 0.0;
    currentShipment.scanned_barcodes.forEach(bc => {
        const w = currentShipment.boxWeights[bc] || 1.0;
        totalW += w;
    });
    currentShipment.calculatedTotalWeight = totalW;

    // Display summary
    el('shipment-summary-display').innerText = `Total Shipment Weight: ${totalW.toFixed(1)} lbs`;
}

// Packing Logic Helpers
function handleCartonInput(id) {
    const validBoxes = ['#105', '#115', '#116', '#160', '#145'];
    let cleanId = id.toUpperCase().trim();
    if (!cleanId.startsWith('#') && cleanId !== 'CUSTOM') {
        if (validBoxes.includes('#' + cleanId)) cleanId = '#' + cleanId;
    }

    if (!validBoxes.includes(cleanId) && cleanId !== 'CUSTOM') {
        showStatus(el('carton-status'), `INVALID BOX CODE: ${id}`, 'error');
        return;
    }

    // UI Selection Update
    document.querySelectorAll('.box-btn').forEach(btn => {
        if (btn.dataset.box === cleanId) btn.classList.add('selected');
        else if (!multiModeCheckbox.checked) btn.classList.remove('selected');
    });

    const finalW = currentShipment.calculatedTotalWeight || 0;

    if (multiModeCheckbox.checked) {
        const w = prompt(`Weight for ${cleanId}?`);
        if (w) {
            packageList.push({ id: cleanId, weight: parseFloat(w) });
        }
    } else {
        // Single Mode
        packageList = [{ id: cleanId, weight: finalW }];
    }
    renderPackedList();
}

function addCustomCarton() {
    const L = parseFloat(el('custom-L').value);
    const W = parseFloat(el('custom-W').value);
    const H = parseFloat(el('custom-H').value);
    const Weight = parseFloat(el('custom-Weight').value);

    if (L && W && H && Weight) {
        const pkg = { id: 'CUSTOM', L, W, H, weight: Weight };
        if (multiModeCheckbox.checked) packageList.push(pkg);
        else packageList = [pkg]; // Replace
        renderPackedList();

        el('custom-L').value = ''; el('custom-W').value = ''; el('custom-H').value = ''; el('custom-Weight').value = '';
    } else {
        alert("Invalid custom box dims");
    }
}

function renderPackedList() {
    const div = el('packed-cartons-list');
    const container = el('packed-list-container');

    div.innerHTML = packageList.map(p => `<span>${p.id} (${p.weight}lbs)</span>`).join(', ');

    if (packageList.length > 0) {
        updateButtonState(el('finish-shipment-btn'), true, 'active-success');
        if (container) container.style.display = 'block';
    } else {
        updateButtonState(el('finish-shipment-btn'), false);
        if (container) container.style.display = 'none';
    }
}

async function finalizeShipment() {
    showStatus(el('carton-status'), 'Processing...', 'warn', false);
    try {
        const res = await fetch('/api/shipment/process', {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                orders: currentShipment.orders,
                scanned_barcodes: Array.from(currentShipment.scanned_barcodes),
                package_list: packageList
            })
        });
        const data = await res.json();

        step4.style.display = 'none'; step1.style.display = 'block';
        el('last-shipment-display').style.display = 'block';
        el('last-shipment-display').innerHTML = `<h3>Shipped: ${data.shipment_uid}</h3>`;

        currentShipment = null;
        orderInput.value = ''; orderInput.focus();
        showStatus(el('status-message'), 'Success', 'success');

    } catch (e) {
        showStatus(el('carton-status'), e.message || 'Error', 'error');
    }
}
