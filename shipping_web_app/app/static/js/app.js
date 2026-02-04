
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
    // Soft Reset: Clear state but keep "Last Shipped" display
    currentShipment = null;
    packageList = [];
    appMode = 'SCANNING_BOXES';

    // Reset UI
    step1.style.display = 'block';
    step2.style.display = 'none';
    step4.style.display = 'none';
    el('address-verification-modal').style.display = 'none';

    // Reset Inputs
    orderInput.value = '';
    boxInput.value = '';
    cartonInput.value = '';

    // Reset Buttons
    updateButtonState(el('process-shipment-btn'), false);

    // Focus
    orderInput.focus();
}

// --- Live Feed Logic ---
async function fetchLiveFeed() {
    try {
        const res = await fetch('/api/activity_feed');
        if (res.ok) {
            const data = await res.json();
            renderFeed(data);
        }
    } catch (e) { console.error("Feed error:", e); }
}

function renderFeed(items) {
    const list = document.getElementById('live-feed-list');
    if (!list) return;
    list.innerHTML = items.map(item => {
        let statusColor = '#007bff';
        if (item.marcom_sync_status === 'SUCCESS') statusColor = '#28a745';
        if (item.marcom_sync_status === 'FAILED' || item.marcom_sync_status === 'ERROR') statusColor = '#dc3545';

        return `
        <li style="background: white; border: 1px solid #ddd; margin-bottom: 10px; padding: 10px; border-radius: 5px; border-left: 5px solid ${statusColor}; list-style:none;">
            <div style="font-weight: bold; font-size: 0.9em; display:flex; justify-content:space-between;">
                <span>${item.job_ticket_number || item.tracking_number}</span>
                <span style="color: #999;">${item.created_at}</span>
            </div>
            <div style="font-size: 0.8em; color: ${statusColor};">
                ${item.marcom_sync_status}: ${(item.marcom_response_message || '').substring(0, 40)}
                ${(item.marcom_response_message || '').includes('Simulated') ? '<span style="color:#666; font-size:0.8em;"> (Simulated)</span>' : ''}
            </div>
        </li>`;
    }).join('');
}

// Initialization
window.onload = function () {
    initBarcodes();
    initListeners();
    if (orderInput) {
        orderInput.value = '';
        orderInput.focus();
    }

    // Start Polling
    setInterval(fetchLiveFeed, 5000);
    fetchLiveFeed();
};

// 2. Global Scan Listener
let scanBuffer = "";
let scanTimeout;
const SCAN_TIMEOUT_MS = 100; // Buffer reset for scanner bursts

// Global Window Listener (Window-level commands)
window.addEventListener('keydown', (e) => {
    // Ignore if modifier keys are pressed
    if (e.ctrlKey || e.altKey || e.metaKey) return;

    // If Enter, process buffer
    if (e.key === 'Enter') {
        if (scanBuffer.length > 2) {
            handleGlobalScan(scanBuffer);
        }
        scanBuffer = "";
        return;
    }

    // If printable char, add to buffer
    if (e.key.length === 1) {
        scanBuffer += e.key;

        clearTimeout(scanTimeout);
        scanTimeout = setTimeout(() => {
            scanBuffer = "";
        }, SCAN_TIMEOUT_MS);
    }
});

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
        if (e.key === 'Enter') {
            const val = orderInput.value.trim();
            if (val && !val.includes('CMD-')) {
                fetchOrderData(val);
            } else if (val.includes('CMD-')) {
                orderInput.value = ''; // Clear command text
            }
        }
    });

    // Autocomplete Logic - Custom Dropdown
    let debounceTimer;
    const acList = document.getElementById('autocomplete-list');

    orderInput.addEventListener('input', (e) => {
        const val = e.target.value.trim();

        // Hide if empty or too short
        if (val.length < 4) {
            acList.style.display = 'none';
            return;
        }

        clearTimeout(debounceTimer);
        debounceTimer = setTimeout(() => {
            fetch(`/api/order/search?q=${encodeURIComponent(val)}`)
                .then(r => r.json())
                .then(suggestions => {
                    acList.innerHTML = ''; // Clear

                    if (suggestions.length === 0) {
                        acList.style.display = 'none';
                        return;
                    }

                    suggestions.forEach(s => {
                        const li = document.createElement('li');
                        li.textContent = s;
                        li.addEventListener('click', () => {
                            // On selection: fill input, hide list, trigger search
                            orderInput.value = s;
                            acList.style.display = 'none';
                            fetchOrderData(s);
                        });
                        acList.appendChild(li);
                    });

                    // Show list
                    acList.style.display = 'block';
                })
                .catch(err => console.error("Autocomplete error:", err));
        }, 300); // 300ms debounce
    });

    // Close list if clicked outside
    document.addEventListener('click', (e) => {
        if (!orderInput.contains(e.target) && !acList.contains(e.target)) {
            acList.style.display = 'none';
        }
    });

    // 3. Step 2 Box Input
    boxInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
            const val = boxInput.value.trim();
            if (!val) return;

            // Standard Processing Logic
            if (val.startsWith('1Z')) {
                showStatus(el('box-scan-status'), 'Tracking number ignored.', 'warn');
                boxInput.value = ''; return;
            }
            if (val.includes('CMD-')) {
                // Let global handler pick it up, just clear input
                boxInput.value = '';
                return;
            }

            if (appMode === 'SCANNING_BOXES') processBoxScan(val);
            else fetchAndCompareOrder(val);
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

    // Modal Listeners
    if (el('modal-confirm-btn')) {
        el('modal-confirm-btn').addEventListener('click', () => {
            el('address-verification-modal').style.display = 'none';
            if (tempNewOrderData) {
                mergeNewOrder(tempNewOrderData);
            }
        });
    }

    if (el('modal-cancel-btn')) {
        el('modal-cancel-btn').addEventListener('click', () => {
            el('address-verification-modal').style.display = 'none';
            tempNewOrderData = null;
            boxInput.value = ''; boxInput.focus();
        });
    }
}

function handleGlobalScan(code) {
    code = code.trim();
    if (!code) return;

    // Helper to clear command text from inputs
    const clearInputs = () => {
        [orderInput, boxInput, cartonInput].forEach(inp => {
            if (inp && inp.value.includes('CMD-')) inp.value = '';
        });
    };

    if (code === 'CMD-CANCEL-ORDER') {
        clearInputs();
        return resetAll();
    }

    // Box Selection (Pattern: #123)
    if (code.startsWith('#') || code === 'CUSTOM') {
        // Only valid if we are in Step 4? 
        // Logic: if step4 is visible, we allow it.
        if (step4.style.display !== 'none') {
            handleCartonInput(code);
            return;
        }
    }

    // Step 2 Commands
    if (step2.style.display !== 'none') {
        if (code === 'CMD-PROCESS') {
            clearInputs();
            if (!el('process-shipment-btn').disabled) goToPackStep();
            return;
        }
    }

    // Step 4 Commands
    if (step4.style.display !== 'none') {
        if (code === 'CMD-FINISH-SHIP') {
            clearInputs();
            if (!el('finish-shipment-btn').disabled) finalizeShipment();
            return;
        }
        if (code === 'CMD-TOGGLE-MULTI') {
            clearInputs();
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
            clearInputs();
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
            orderProgress: data.order_progress || {}, // Ensure object
            status: data.status || 'OPEN'
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

// --- Address Verification Logic ---
let tempNewOrderData = null;

async function fetchAndCompareOrder(newId) {
    // 1. Check if it's already in the shipment?
    // (Logic handled by processBoxScan mostly, but if user scans a NEW Order Number, we land here)

    showStatus(el('box-scan-status'), 'Verifying Order...', 'warn', false);

    // Clean ID
    newId = newId.trim();

    try {
        const res = await fetch('/api/order/compare', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                current_address: currentShipment.ship_to,
                new_order_id: newId
            })
        });

        if (!res.ok) throw await res.json();
        const data = await res.json();

        if (data.status === 'mismatch') {
            // Trigger Modal
            tempNewOrderData = data.new_order; // Hold for confirmation

            el('new-order-id-modal').innerText = newId;
            el('current-address-display').innerText = formatAddr(currentShipment.ship_to);
            el('new-address-display').innerText = formatAddr(data.new_order.ship_to);

            el('address-verification-modal').style.display = 'flex';

        } else {
            // Match (Exact or Fuzzy) - Auto Merge
            showStatus(el('box-scan-status'), 'Address Matched. Merging...', 'success');
            mergeNewOrder(data.new_order);
        }

    } catch (e) {
        // If it's just a barcode error, maybe it's not an order?
        // Fallback to "Invalid Barcode" if API fails (404)
        showStatus(el('box-scan-status'), e.message || 'Invalid Order/Barcode', 'error');
    }
}

function formatAddr(a) {
    return `${a.company}\n${a.name}\n${a.address1}\n${a.city}, ${a.state} ${a.zip}`;
}

function mergeNewOrder(newOrder) {
    // Add to currentShipment
    currentShipment.orders.push(newOrder);

    // Add expected barcodes
    // (Make sure to avoid duplicates if re-scanning same order)
    newOrder.expected_barcodes.forEach(bc => {
        if (!currentShipment.all_expected_barcodes.includes(bc)) {
            currentShipment.all_expected_barcodes.push(bc);
        }
    });

    // Re-render
    updateBarcodeList();
    showStatus(el('box-scan-status'), `Order ${newOrder.order_number} added!`, 'success');
    boxInput.value = ''; boxInput.focus();
    tempNewOrderData = null;
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

    // Status Banner
    const statusEl = el('order-status-display');
    if (currentShipment.status === 'COMPLETED') {
        statusEl.textContent = '✅ ORDER SHIPPED';
        statusEl.style.background = '#d4edda';
        statusEl.style.color = '#155724';
        statusEl.style.border = '1px solid #c3e6cb';
        statusEl.style.display = 'block';
    } else if (currentShipment.status === 'PARTIALLY SHIPPED') {
        statusEl.textContent = '⚠️ PARTIALLY SHIPPED';
        statusEl.style.background = '#fff3cd';
        statusEl.style.color = '#856404';
        statusEl.style.border = '1px solid #ffeeba';
        statusEl.style.display = 'block';
    } else {
        statusEl.style.display = 'none';
    }
}

function updateBarcodeList() {
    // 1. Calculate Totals first
    let grandTotal = 0;
    let grandPacked = 0;

    const allItems = currentShipment.orders.flatMap(o => o.line_items || []);
    allItems.forEach(i => i.barcodes.forEach(b => {
        grandTotal++;
        if (b.status === 'packed' || currentShipment.scanned_barcodes.has(b.value)) grandPacked++;
    }));

    // Update Progress Bar
    const pct = grandTotal > 0 ? (grandPacked / grandTotal) * 100 : 0;
    el('scan-progress-bar').style.width = `${pct}%`;
    el('scan-progress-text').textContent = `${grandPacked} / ${grandTotal}`;

    // Header Dynamic Update
    const activeOrderNum = currentShipment.orders[0].related_order_number || currentShipment.orders[0].order_number;
    const h1 = step2.querySelector('h1');
    if (h1) h1.innerHTML = `Scanning Items for <span style="color:#007bff; font-weight:bold;">${activeOrderNum}</span>`;

    // 2. Render Job Groups
    el('expected-barcodes').innerHTML = ''; // Clear List

    const grouped = {};
    allItems.forEach(i => {
        const k = i.job_ticket || "Items";
        if (!grouped[k]) grouped[k] = [];
        grouped[k].push(i);
    });

    Object.keys(grouped).sort().forEach(jt => {
        const groupItems = grouped[jt];

        // Determine Job Status & Dates
        let allJobBoxesPacked = true;
        let latestPackDate = "";

        groupItems.forEach(item => {
            item.barcodes.forEach(bc => {
                const isScanned = currentShipment.scanned_barcodes.has(bc.value);
                const isPacked = bc.status === 'packed';
                // Effectively packed if either server says so OR local scan says so (though we use server date usually)
                if (!isPacked && !isScanned) allJobBoxesPacked = false;

                if (bc.packed_at && bc.packed_at > latestPackDate) latestPackDate = bc.packed_at;
            });
        });

        // Job Container (Frame)
        const jobContainer = document.createElement('div');
        const borderColor = allJobBoxesPacked ? "#28a745" : "#007bff";
        jobContainer.style.border = `2px solid ${borderColor}`; // Blue or Green
        jobContainer.style.borderRadius = "8px";
        jobContainer.style.marginBottom = "30px";
        jobContainer.style.backgroundColor = "#fff";
        jobContainer.style.overflow = "hidden";
        jobContainer.style.boxSizing = "border-box";

        // Job Header
        const jobHeader = document.createElement('div');
        jobHeader.style.backgroundColor = allJobBoxesPacked ? "#d4edda" : "#e3f2fd";
        jobHeader.style.padding = "10px 15px";
        jobHeader.style.borderBottom = `1px solid ${allJobBoxesPacked ? "#c3e6cb" : "#90caf9"}`;
        jobHeader.style.display = "flex";
        jobHeader.style.justifyContent = "space-between";
        jobHeader.style.alignItems = "center";

        // Format Date Helper
        const formatDate = (dateStr) => {
            if (!dateStr) return '';
            const [y, m, d] = dateStr.split('-');
            return `${m}/${d}/${y}`;
        };

        const shippedText = allJobBoxesPacked ? `<span style="color:#155724; font-weight:bold;">Shipped on ${formatDate(latestPackDate)}</span>` : "";

        jobHeader.innerHTML = `<h2 style="margin:0; font-size:1.4em; color:${allJobBoxesPacked ? "#155724" : "#0d47a1"};">${jt} ${shippedText}</h2>
                               <span style="font-size:0.9em; color:#555;">${groupItems.length} Line Item(s)</span>`;

        jobContainer.appendChild(jobHeader);

        // Items Container
        const itemsList = document.createElement('div');
        itemsList.style.padding = "15px";

        groupItems.forEach((item, idx) => {
            const itemDiv = document.createElement('div');
            itemDiv.style.marginBottom = (idx === groupItems.length - 1) ? "0" : "20px";

            // Check Item Status
            let allItemBoxesPacked = true;
            let itemPackDate = null;
            item.barcodes.forEach(bc => {
                const isScanned = currentShipment.scanned_barcodes.has(bc.value);
                const isPacked = bc.status === 'packed';
                if (!isPacked && !isScanned) allItemBoxesPacked = false;
                if (bc.packed_at) itemPackDate = bc.packed_at;
            });

            // Item Title
            itemDiv.innerHTML = `
                <div style="margin-bottom:10px; border-bottom:1px dashed #eee; padding-bottom:5px;">
                     <div style="font-weight:bold; font-size:1.1em; margin-bottom:4px; line-height:1.2;">
                        ${item.sku_description || 'Item'}
                     </div>
                     <div style="font-size:1.0em; color:#333; line-height:1.2;">
                        ${item.sku}
                     </div>
                </div>
            `;

            // Content Row
            const contentRow = document.createElement('div');
            contentRow.style.display = "flex";
            contentRow.style.justifyContent = "space-between";
            contentRow.style.alignItems = "flex-start";

            // Barcodes Grid
            const bcContainer = document.createElement('div');
            bcContainer.className = 'barcode-grid';
            bcContainer.style.display = "flex";
            bcContainer.style.gap = "10px";
            bcContainer.style.flexWrap = "wrap";
            bcContainer.style.justifyContent = 'flex-start';
            bcContainer.style.flex = "1";

            item.barcodes.forEach(bcObj => {
                const code = bcObj.value;
                const isScanned = currentShipment.scanned_barcodes.has(code);
                const isPacked = bcObj.status === 'packed';

                // Styles
                let bg = '#fff';
                let border = '#e0e0e0';

                if (isPacked) {
                    bg = '#e2e6ea';
                    border = '#adb5bd';
                } else if (isScanned) {
                    bg = '#d4edda';
                    border = '#28a745';
                }

                const weight = bcObj.estimated_weight || 1.0;

                bcContainer.innerHTML += `
                <div class="barcode-card" style="width:140px; padding:8px 10px; background:${bg}; border:2px solid ${border}; border-radius:8px; transition:0.2s; min-height:auto; opacity:${isPacked ? 0.8 : 1}; text-align:center;">
                     <span class="barcode-label" style="font-size:1.0em; font-weight:bold; display:block; margin-bottom:2px;">${code}</span>
                     <span style="font-size:0.8em; color:#666;">${weight} lbs</span>
                     ${isPacked ? '<div style="font-size:0.7em; color:#28a745; font-weight:bold; margin-top:2px;">PACKED</div>' : ''}
                </div>`;
            });

            contentRow.appendChild(bcContainer);

            // Qty/Status Text
            const qtyDiv = document.createElement('div');
            qtyDiv.style.marginLeft = "20px";
            qtyDiv.style.textAlign = "right";

            let statusHtml = "";
            if (allItemBoxesPacked) {
                statusHtml = `<div style="color:#28a745; font-size:0.85em; margin-bottom:5px;">Packed on ${formatDate(itemPackDate) || 'Just Now'}</div>`;
            }

            qtyDiv.innerHTML = `
                ${statusHtml}
                <div style="background:#eee; padding:5px 10px; border-radius:4px; display:inline-block;">
                    Qty: <strong>${item.quantity_ordered}</strong>
                </div>
            `;

            contentRow.appendChild(qtyDiv);
            itemDiv.appendChild(contentRow);
            itemsList.appendChild(itemDiv);
        });

        jobContainer.appendChild(itemsList);
        el('expected-barcodes').appendChild(jobContainer);
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
    let isAlreadyPacked = false;
    currentShipment.orders.forEach(o => {
        if (o.line_items) o.line_items.forEach(li => {
            li.barcodes.forEach(bc => {
                if (bc.value === code && bc.status === 'packed') isAlreadyPacked = true;
            });
        });
    });

    if (isAlreadyPacked) {
        return showStatus(el('box-scan-status'), `ALREADY SHIPPED: ${code}`, 'error'), boxInput.value = '';
    }

    currentShipment.scanned_barcodes.add(code);
    updateBarcodeList();
    showStatus(el('box-scan-status'), `OK: ${code}`, 'success');

    checkProcessShipmentEligibility();


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

    // Removed "Est Weight" prominence on step 4 as per request
    const container = el('pack-screen-buttons');
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
        el('last-shipment-display').innerHTML = `
            <div style="background:#d4edda; border:1px solid #155724; color:#155724; padding:15px; border-radius:5px; margin-bottom:20px;">
                <h3 style="margin-top:0;">✅ Last Shipment: ${data.shipment_uid}</h3>
                <p>Packages: ${packageList.length}</p>
            </div>
        `;

        // Soft Reset State instead of reload
        currentShipment = null;
        packageList = [];
        // Keep last-shipment-display visible!

        orderInput.value = ''; orderInput.focus();
        showStatus(el('status-message'), 'Success! Ready for next order.', 'success');

    } catch (e) {
        showStatus(el('carton-status'), e.message || 'Error', 'error');
    }
}
