import {
        openModal,
        closeModal
}
from "./popup.js";

const currentFlows = [];
const currentFlowContainer = document.getElementById("currentFlowContainer");
const currentFlowTable = document.getElementById("currentFlowTable");
const currentFlowCountEl = document.getElementById("count-CurrentFlow");

export function updateCurrentFlowTable() {
        if (!currentFlowTable || !currentFlowCountEl) return;
        if (currentFlows.length === 0) {
                currentFlowTable.innerHTML = `<tr><td class="p-2 border-b text-center" colspan="7">No current flows available</td></tr>`;
                currentFlowCountEl.textContent = "Count : 0";
                return;
        }
        // Clear existing rows
        currentFlowTable.innerHTML = "";

        // Add rows for each current flow
        // Sort flows by lastSeen in descending order (most recent on top)
        currentFlows.sort((a, b) => new Date(b.lastSeen) - new Date(a.lastSeen));

        currentFlows.forEach(flow => {
                const row = document.createElement("tr");
                row.classList.add("hover:bg-gray-100", "cursor-pointer");
                row.innerHTML = `
                                <td class="p-2 border-b ">${new Date(flow.firstSeen).toLocaleString()}</td>
                                <td class="p-2 border-b ">${new Date(flow.lastSeen).toLocaleString()}</td>
                                <td class="p-2 border-b ">${flow.srcIp}</td>
                                <td class="p-2 border-b ">${flow.dstIp}</td>
                                <td class="p-2 border-b ">${flow.srcPort}</td>
                                <td class="p-2 border-b ">${flow.dstPort}</td>
                                <td class="p-2 border-b ">${flow.protocol}</td>`;

                currentFlowTable.appendChild(row);

                row.addEventListener("click", () => {
                        showFlowDetails(flow);
                });
        });
        // Update count display
        currentFlowCountEl.textContent = `Count : ${currentFlows.length}`;

}

export function handleCurrentFlowData(data) {
        // Update the currentFlows array
        currentFlows.length = 0; // Clear existing data
        data.flows.forEach(flow => currentFlows.push(flow));

        // Update the table display
        updateCurrentFlowTable();
}

function showFlowDetails(flow) {
        const title = `Flow Details - ${flow.flowKey}`;
        openModal(title, flow, "json");
}