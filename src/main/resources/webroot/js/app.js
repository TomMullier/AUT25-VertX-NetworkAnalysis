import {
        severityCount,
        riskCount,
        protocolCount,
        countryLinks,
        incrementCount,
        addCountryLink,
        updateCharts,
        initCharts,
        updateMapLinks,
        initMap,
        fetchCountryCoords
} from "./stats.js";;

import {
        fetchIngestionMethod,
        updatePcapAndInterfacesVisibility
} from "./changeSettings.js";

import {
        openModal,
        closeModal
}
from "./popup.js";

import {
        handleCurrentFlowData
} from "./currentFlows.js";

import {
        handleMalformedPacketData
} from "./malformedPackets.js";



/* -------------------------------- VARIABLE -------------------------------- */
const ws = new WebSocket(`ws://${location.host}/`);
const statusEl = document.getElementById("connectionStatus");
const flowTableBodyEmergency = document.getElementById("table-Emergency");
const flowTableBodyCritical = document.getElementById("table-Critical");
const flowTableBodyHigh = document.getElementById("table-High");
const flowTableBodySevere = document.getElementById("table-Severe");
const flowTableBodyMedium = document.getElementById("table-Medium");
const flowTableBodyLow = document.getElementById("table-Low");
const flowTableBodyNoRisk = document.getElementById("table-None");

const flowTableEmergencyCounter = document.getElementById("count-Emergency");
const flowTableCriticalCounter = document.getElementById("count-Critical");
const flowTableHighCounter = document.getElementById("count-High");
const flowTableSevereCounter = document.getElementById("count-Severe");
const flowTableMediumCounter = document.getElementById("count-Medium");
const flowTableLowCounter = document.getElementById("count-Low");
const flowTableNoRiskCounter = document.getElementById("count-None");

const tables = [
        flowTableBodyEmergency,
        flowTableBodyCritical,
        flowTableBodyHigh,
        flowTableBodySevere,
        flowTableBodyMedium,
        flowTableBodyLow,
        flowTableBodyNoRisk
];

const counts = {
        "Emergency": 0,
        "Critical": 0,
        "High": 0,
        "Severe": 0,
        "Medium": 0,
        "Low": 0,
        "None": 0
};

const packetsEl = document.getElementById("packetContainer");

const flowsList = [];
const flowsLimit = 100;

/* ----------------------------- WEBSOCKET LOGIC ----------------------------- */
ws.onopen = () => {
        statusEl.textContent = "🟢 Connected to WebSocket server";
        statusEl.classList.remove("text-red-600");
        statusEl.classList.add("text-green-600");

        console.log("WebSocket connection established : ", ws);
};

ws.onmessage = (event) => {
        try {
                const data = JSON.parse(event.data);

                if (data.type === "flow") {
                        addFlowRow(data);

                        // === Risque et sévérité ===
                        const severity = data.riskSeverity;
                        const riskLabel = data.riskLabel ? data.riskLabel.split(", ") : [];


                        if (severity) {
                                if (severity == "No risk") {
                                        incrementCount(severityCount, "NoRisk");
                                } else if (severity != "Unknown (ARP)") {
                                        incrementCount(severityCount, severity);
                                }
                        }
                        riskLabel.forEach(label => {
                                if (label && label.trim() !== "") incrementCount(riskCount, label);
                        });

                        // === Protocoles ===
                        const protocolKey = data.appProtocol || data.protocol || "Unknown";
                        incrementCount(protocolCount, protocolKey);



                        // === Carte du trafic ===
                        const srcCountry = data.srcCountry || "Unknown";
                        const dstCountry = data.dstCountry || "Unknown";
                        addCountryLink(srcCountry, dstCountry);
                        updateMapLinks();

                        // === Rafraîchir les graphiques ===
                        updateCharts();

                } else if (data.type === "packet") {
                        //ToDO No display for packets because of performances issues with high ingestion rates 

                        // const pre = document.createElement("pre");
                        // pre.textContent = JSON.stringify(data, null, 2);
                        // pre.className = "bg-gray-100 p-2 rounded mb-2 overflow-x-auto text-sm";
                        // packetsEl.prepend(pre);
                } else if (data.type === "currentFlow") {
                        handleCurrentFlowData(data);
                } else if (data.type === "malformedPacket") {
                        handleMalformedPacketData(data);
                } else {
                        console.warn("Unknown WebSocket message type:", data.type);
                }
        } catch (e) {
                console.error("Error processing WebSocket message:" + event.data, e);
        }
};

ws.onerror = (err) => {
        statusEl.textContent = "❌ WebSocket error: " + err.message;
        statusEl.classList.remove("text-green-600");
        statusEl.classList.add("text-red-600");

        console.error("WebSocket error:", err);
};

ws.onclose = () => {
        statusEl.textContent = "🔴 Disconnected from WebSocket server";
        statusEl.classList.remove("text-green-600");
        statusEl.classList.add("text-red-600");

        console.warn("WebSocket connection closed");
};

/* ------------------------------- Table flows ------------------------------ */
function flowClickedInfos(flow) {
        const title = `Flow Details - ${flow.flowKey}`;
        openModal(title, flow, "json");
}


/**
 * Add a flow row to the table
 * @param {*} flow 
 */
function addFlowRow(flow) {
        // Maintain only the latest flowsLimit flows
        flowsList.push(flow);

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
                flow.appProtocol,
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
                td.className = "p-2 border-b";
                row.classList.add("hover:bg-gray-100", "cursor-pointer");
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

        // Maintain only the latest flowsLimit flows for every table 
        tables.forEach(table => {
                while (table.children.length >= flowsLimit) {
                        table.removeChild(table.lastChild);
                }
        });


        // Add cells to the row
        const filter = document.getElementById("searchInput").value.toLowerCase();
        const text = row.textContent.toLowerCase();
        row.style.display = text.includes(filter) ? "" : "none";
        // Prepend the new row to have the latest on top
        row.addEventListener('click', flowClickedInfos.bind(null, flow));
        switch (flow.riskSeverity) {
                case "Emergency":
                        flowTableBodyEmergency.prepend(row);
                        counts["Emergency"] += 1;
                        flowTableEmergencyCounter.innerText = `Count : ${counts["Emergency"]}`;
                        break;
                case "Critical":
                        flowTableBodyCritical.prepend(row);
                        counts["Critical"] += 1;
                        flowTableCriticalCounter.innerText = `Count : ${counts["Critical"]}`;
                        break;
                case "High":
                        flowTableBodyHigh.prepend(row);
                        counts["High"] += 1;
                        flowTableHighCounter.innerText = `Count : ${counts["High"]}`;
                        break;
                case "Severe":
                        flowTableBodySevere.prepend(row);
                        counts["Severe"] += 1;
                        flowTableSevereCounter.innerText = `Count : ${counts["Severe"]}`;
                        break;
                case "Medium":
                        flowTableBodyMedium.prepend(row);
                        counts["Medium"] += 1;
                        flowTableMediumCounter.innerText = `Count : ${counts["Medium"]}`;
                        break;
                case "Low":
                        flowTableBodyLow.prepend(row);
                        counts["Low"] += 1;
                        flowTableLowCounter.innerText = `Count : ${counts["Low"]}`;
                        break;
                default:
                        flowTableBodyNoRisk.prepend(row);
                        counts["None"] += 1;
                        flowTableNoRiskCounter.innerText = `Count : ${counts["None"]}`;
                        break;
        }
        updateTableVisibility();

}

/** 
 * Search filter for flow table
 */
document.getElementById("searchInput").addEventListener("input", function () {
        const filter = this.value.toLowerCase();

        tables.forEach(table => {
                const rows = table.querySelectorAll("tr");
                rows.forEach(row => {
                        const text = row.textContent.toLowerCase();
                        row.style.display = text.includes(filter) ? "" : "none";
                });
        });
        updateTableVisibility();

});


/**
 * This function handles the reset of the dashboard, clearing all data and resetting the input fields.
 */
document.getElementById("resetButton").addEventListener("click", () => {
        const searchInput = document.getElementById("searchInput");
        searchInput.value = "";

        tables.forEach(table => {
                table.innerHTML = "";
        });
        updateTableVisibility();

        // const packetContainer = document.getElementById("packetContainer");
        // packetContainer.innerHTML = "";

        document.getElementById("resetMessage").textContent =
                "Cleared all data from dashboard.";
        setTimeout(() => {
                document.getElementById("resetMessage").textContent = "";
        }, 3000);
});

function updateTableVisibility() {
        // Update visibiity of table if items are in body 
        tables.forEach(table => {
                if (table.children.length === 0) {
                        table.parentElement.parentElement.parentElement.style.display = "none";
                } else {
                        table.parentElement.parentElement.parentElement.style.display = "";
                }
        });
}

/**
 * DOMContentLoaded event to initialize settings and charts
 */
document.addEventListener("DOMContentLoaded", async () => {
        initCharts();
        initMap(); // Initialise Leaflet
        await fetchCountryCoords(); // Récupère toutes les coordonnées

        updateTableVisibility();
        await fetchIngestionMethod();

        const ingestionRadios = document.querySelectorAll('input[name="ingestionMethod"]');

        // Add change event listeners to ingestion method radios
        ingestionRadios.forEach(radio => {
                radio.addEventListener("change", updatePcapAndInterfacesVisibility);
        });

        // Initial update on page load
        await updatePcapAndInterfacesVisibility();
});




function toggleSection(sectionId, arrowIconId) {
        const section = document.getElementById(sectionId);
        if (section.children[1].style.display === "none") {
                section.children[1].style.display = "block";
                document.getElementById(arrowIconId).innerText = "▼";
        } else {
                section.children[1].style.display = "none";
                document.getElementById(arrowIconId).innerText = "▶";
        }
}

document.getElementById('settingsToggle').addEventListener('click', () => {
        toggleSection("settings", "settingsArrow");

});
document.getElementById("currentFlowTitle").addEventListener("click", () => {
        toggleSection("currentFlowContainer", "arrow-CurrentFlow");
});
document.getElementById("malformedPacketTitle").addEventListener("click", () => {
        toggleSection("malformedPacketContainer", "arrow-MalformedPacket");
});