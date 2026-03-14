import {
        openModal,
        closeModal
}
from "./popup.js";


const malformedPacketTable = document.getElementById("malformedPacketTable");
const malformedPacketContainer = document.getElementById("malformedPacketContainer");
let malformedPackets = [];

export function handleMalformedPacketData(data) {
        // Update the malformedPackets array
        malformedPackets.push(data);

        // Add a new row to the table
        addMalformedPacketRow({
                error: data.error,
                rawData: data.rawData
        });
        // Update the count display
        updateMalformedPacketCount();
}

export function addMalformedPacketRow(cells) {
        // Remove "No malformed packets available" row if it exists
        if (malformedPacketTable.rows.length === 1 &&
                malformedPacketTable.rows[0].cells.length === 1) {
                malformedPacketTable.deleteRow(0);
        }

        const row = malformedPacketTable.insertRow(0); // Insert at the top
        row.classList.add("hover:bg-gray-100", "cursor-pointer");
        row.addEventListener("click", () => {
                const title = `Malformed Packet Details`;
                openModal(title, cells, "json");
        });
        const cellValues = [
                cells.error,
                cells.rawData
        ];

        cellValues.forEach((value) => {
                const td = document.createElement("td");
                td.textContent = value !== undefined ? value : "-";
                td.className = "p-2 border-b";
                row.appendChild(td);
        });
}

export function updateMalformedPacketCount() {
        if (!malformedPacketContainer) return;
        let countEl = malformedPacketContainer.querySelector("#count-MalformedPacket");
        if (!countEl) {
                // Create count element if it doesn't exist
                countEl = document.createElement("span");
                countEl.id = "count-MalformedPacket";
                countEl.className = "font-semibold";
                malformedPacketContainer.querySelector("h3").appendChild(countEl);
        }
        countEl.textContent = `Count : ${malformedPackets.length}`;
}