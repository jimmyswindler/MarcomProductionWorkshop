/**
 * Shared Sortable Table Utility
 * 
 * Auto-initializes on any table containing th.sortable headers.
 * Uses data-sort attribute on <th> for column identity and
 * data-sort-val attribute on <td> for sort values.
 *
 * Usage:
 *   1. Add class="sortable" and data-sort="columnName" to sortable <th> elements.
 *   2. Add data-sort-val="rawValue" to corresponding <td> elements.
 *   3. Include this script on the page.
 *   4. Optionally set data-default-sort="columnName" and
 *      data-default-sort-dir="asc|desc" on the <table> element.
 */
document.addEventListener('DOMContentLoaded', () => {
    document.querySelectorAll('table').forEach(table => {
        const headers = table.querySelectorAll('th.sortable');
        if (headers.length === 0) return;

        const tbody = table.querySelector('tbody');
        if (!tbody) return;

        const allThs = Array.from(table.querySelectorAll('thead th'));

        // Read default sort from table data attributes
        let currentSortColumn = table.dataset.defaultSort || null;
        let currentSortAsc = (table.dataset.defaultSortDir || 'asc') === 'asc';

        /**
         * Sort table rows by the given column name and direction.
         */
        const sortTable = (column, asc) => {
            const rows = Array.from(tbody.querySelectorAll('tr'));

            // Don't sort if it's an empty-state row (colspan)
            if (rows.length <= 1 && rows[0] && rows[0].querySelector('td[colspan]')) return;

            // Find the absolute column index using the header's position in all <th>s
            let colIndex = -1;
            headers.forEach(h => {
                if (h.dataset.sort === column) {
                    colIndex = allThs.indexOf(h);
                }
            });
            if (colIndex === -1) return;

            rows.sort((a, b) => {
                const cellA = a.querySelectorAll('td')[colIndex];
                const cellB = b.querySelectorAll('td')[colIndex];

                if (!cellA || !cellB) return 0;

                // Prefer data-sort-val; fall back to textContent
                let valA = cellA.dataset.sortVal !== undefined ? cellA.dataset.sortVal : cellA.textContent.trim();
                let valB = cellB.dataset.sortVal !== undefined ? cellB.dataset.sortVal : cellB.textContent.trim();

                // Numeric comparison (integers, decimals, store numbers like "0237")
                const numA = Number(valA);
                const numB = Number(valB);
                if (valA !== '' && valB !== '' && !isNaN(numA) && !isNaN(numB)) {
                    return asc ? numA - numB : numB - numA;
                }

                // Date comparison (only for ISO-style date strings, e.g. "2026-03-09")
                if (/^\d{4}-\d{2}-\d{2}/.test(valA) && /^\d{4}-\d{2}-\d{2}/.test(valB)) {
                    const dateA = Date.parse(valA);
                    const dateB = Date.parse(valB);
                    if (!isNaN(dateA) && !isNaN(dateB)) {
                        return asc ? dateA - dateB : dateB - dateA;
                    }
                }

                // String comparison (case-insensitive)
                valA = String(valA).toLowerCase();
                valB = String(valB).toLowerCase();
                if (valA < valB) return asc ? -1 : 1;
                if (valA > valB) return asc ? 1 : -1;
                return 0;
            });

            // Re-append sorted rows
            rows.forEach(row => tbody.appendChild(row));
        };

        /**
         * Update header classes and caret indicators.
         */
        const updateHeaderUI = () => {
            headers.forEach(h => {
                h.classList.remove('sort-asc', 'sort-desc');
                // Remove any existing caret
                const existingCaret = h.querySelector('.sort-caret');
                if (existingCaret) existingCaret.remove();

                if (h.dataset.sort === currentSortColumn) {
                    h.classList.add(currentSortAsc ? 'sort-asc' : 'sort-desc');
                    const caret = document.createElement('span');
                    caret.className = 'sort-caret';
                    caret.textContent = currentSortAsc ? ' ▲' : ' ▼';
                    h.appendChild(caret);
                }
            });
        };

        // Apply default sort on load
        if (currentSortColumn) {
            updateHeaderUI();
            sortTable(currentSortColumn, currentSortAsc);
        }

        // Click handlers
        headers.forEach(header => {
            header.addEventListener('click', () => {
                const column = header.dataset.sort;

                if (currentSortColumn === column) {
                    currentSortAsc = !currentSortAsc;
                } else {
                    currentSortColumn = column;
                    currentSortAsc = true;
                }

                updateHeaderUI();
                sortTable(currentSortColumn, currentSortAsc);
            });
        });
    });
});
