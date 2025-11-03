const ws = new WebSocket(`ws://${location.host}/`);

const statusEl = document.getElementById("connectionStatus");
const flowTableBody = document.getElementById("flowTableBody");
const packetsEl = document.getElementById("packetContainer");

ws.onopen = () => {
        statusEl.textContent = "🟢 Connected to WebSocket server";
        statusEl.classList.remove("text-red-600");
        statusEl.classList.add("text-green-600");
};

ws.onmessage = (event) => {
        try {
                const data = JSON.parse(event.data);

                if (data.type === "flow") {
                        addFlowRow(data);
                } else if (data.type === "packet") {
                        const pre = document.createElement("pre");
                        pre.textContent = JSON.stringify(data, null, 2);
                        pre.className = "bg-gray-100 p-2 rounded mb-2 overflow-x-auto text-sm";
                        packetsEl.prepend(pre);
                }
        } catch (e) {
                console.error("Error processing WebSocket message:", e);
        }
};

ws.onerror = (err) => {
        statusEl.textContent = "❌ WebSocket error: " + err.message;
        statusEl.classList.remove("text-green-600");
        statusEl.classList.add("text-red-600");
};

ws.onclose = () => {
        statusEl.textContent = "🔴 Disconnected from WebSocket server";
        statusEl.classList.remove("text-green-600");
        statusEl.classList.add("text-red-600");
};

/* ------------------------------- Table flows ------------------------------ */
function addFlowRow(flow) {
        const row = document.createElement("tr");

        const formatDate = (ts) => {
                const d = new Date(ts);
                return d.toLocaleString("fr-FR", {
                        hour12: false
                }) + "." + d.getMilliseconds();
        };

        // Cells content
        const riskInfo = flow.riskLabel === "No risk" ?
                "No risk" :
                `${flow.riskLabel.replace(/,/g, '<br>')}<br>(${flow.riskSeverity.toUpperCase()})`;
        const cells = [
                formatDate(flow.firstSeen),
                formatDate(flow.lastSeen),
                flow.srcIp,
                flow.dstIp,
                flow.srcPort,
                flow.dstPort,
                flow.srcCountry,
                flow.dstCountry,
                flow.srcOrg,
                flow.dstOrg,
                flow.protocol,
                flow.packetCount,
                flow.bytes,
                flow.flowDurationMs,
                riskInfo,
                flow.flowKey
        ];

        // Add cells to the row
        for (let i = 0; i < cells.length; i++) {
                const td = document.createElement("td");
                if (i === 14) {
                        td.innerHTML = cells[i] !== undefined ? cells[i] : "-";
                } else {
                        td.textContent = cells[i] !== undefined ? cells[i] : "-";
                }
                td.className = "p-2 border";
                // 🎨 Color on the risk column
                if (i === 14) {
                        td.classList.add("font-semibold");
                        if (flow.riskSeverity === "Emergency") td.classList.add("text-red-900");
                        else if (flow.riskSeverity === "Critical") td.classList.add("text-red-700");
                        else if (flow.riskSeverity === "High") td.classList.add("text-orange-600");
                        else if (flow.riskSeverity === "Severe") td.classList.add("text-yellow-300");
                        else if (flow.riskSeverity === "Medium") td.classList.add("text-green-500");
                        else if (flow.riskSeverity === "Low") td.classList.add("text-blue-500");
                        else td.classList.add("text-gray-500");
                }
                row.appendChild(td);
        }

        // Add cells to the row
        flowTableBody.prepend(row);

        // Keep max 100 rows
        // if (flowTableBody.children.length > 100) {
        //         flowTableBody.removeChild(flowTableBody.lastChild);
        // }
}

// Table filtering function
document.getElementById("searchInput").addEventListener("input", function () {
        const filter = this.value.toLowerCase();
        const rows = document.querySelectorAll("#flowTableBody tr");

        rows.forEach(row => {
                const text = row.textContent.toLowerCase();
                row.style.display = text.includes(filter) ? "" : "none";
        });
});


// Reset button: clear search and clear data 
document.getElementById("resetButton").addEventListener("click", () => {
        // 1. Reset input field
        const searchInput = document.getElementById("searchInput");
        searchInput.value = "";
        const rows = document.querySelectorAll("#flowTableBody tr");
        rows.forEach(row => (row.style.display = ""));

        // 2. Clear flow table
        const flowBody = document.getElementById("flowTableBody");
        flowBody.innerHTML = "";

        // 3. Clear packets
        const packetContainer = document.getElementById("packetContainer");
        packetContainer.innerHTML = "";

        document.getElementById("resetMessage").textContent =
                "Cleared all data from dashboard.";
        setTimeout(() => {
                document.getElementById("resetMessage").textContent = "";
        }, 3000);
});