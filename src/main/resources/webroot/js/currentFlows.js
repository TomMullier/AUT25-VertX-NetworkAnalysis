const currentFlows = [];
const currentFlowTable = document.getElementById("currentFlowTable");
const currentFlowCountEl = document.getElementById("count-CurrentFlow");

export function updateCurrentFlowTable() {
        // Clear existing rows
        currentFlowTable.innerHTML = "";

        // Add rows for each current flow
        // Sort flows by lastSeen in descending order (most recent on top)
        currentFlows.sort((a, b) => new Date(b.lastSeen) - new Date(a.lastSeen));

        currentFlows.forEach(flow => {
                const row = document.createElement("tr");

                row.innerHTML = `
                                <td>${new Date(flow.firstSeen).toLocaleString()}</td>
                                <td>${new Date(flow.lastSeen).toLocaleString()}</td>
                                <td>${flow.srcIp}</td>
                                <td>${flow.dstIp}</td>
                                <td>${flow.srcPort}</td>
                                <td>${flow.dstPort}</td>
                                <td>${flow.protocol}</td>`;


                currentFlowTable.appendChild(row);
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