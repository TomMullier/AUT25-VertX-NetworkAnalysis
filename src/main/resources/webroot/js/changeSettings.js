/**
 * This function updates the settings on the server.
 * @param {*} settings 
 */
async function updateSettings(settings) {
        try {
                const response = await fetch("/api/settings", {
                        method: "POST",
                        headers: {
                                "Content-Type": "application/json"
                        },
                        body: JSON.stringify(settings)
                });

                if (response.ok) {
                        console.log("[SETTINGS UPDATED SUCCESSFULLY]" + JSON.stringify(settings));
                } else {
                        console.error("Failed to update settings");
                }
        } catch (err) {
                console.error("Error updating settings:", err);
        }
}

/**
 *  This function checks if a given PCAP file exists on the server.
 * @param {*} fileName 
 * @returns {Promise<boolean>}
 */
async function checkFileExists(fileName) {
        try {
                const response = await fetch(`/api/checkFileExists?file=${encodeURIComponent(fileName)}`);
                if (response.ok) {
                        const data = await response.json();
                        return data.exists;
                } else {
                        console.error("Failed to check file existence");
                        return false;
                }
        } catch (err) {
                console.error("Error checking file existence:", err);
                return false;
        }
}

/**
 * This function handles the submission of the settings form, preventing the default action and gathering the input values.
 * It also validates the input and updates the settings on the server.
 */
document.getElementById("SettingsForm").addEventListener("submit", async (e) => {
        e.preventDefault();
        let method = "";
        const radios = document.getElementsByName("ingestionMethod");
        for (const radio of radios) {
                if (radio.checked) {
                        method = radio.value;
                        break;
                }
        }

        if (!method) {
                console.log("Please select an ingestion method");
                return;
        }

        //If pcap mode is selected, ensure a file is chosen and that it exists
        let filePath = "";

        if (method === "pcap") {
                const pcapSelect = document.getElementById("pcapFile");
                const selectedFile = pcapSelect.value;
                if (!selectedFile) {
                        console.log("Please select a PCAP file");
                        return;
                }

                const fileExists = await checkFileExists(selectedFile);
                if (!fileExists) {
                        console.log("The selected PCAP file does not exist");
                        return;
                }

                console.log("Selected PCAP file:", selectedFile);
                filePath = selectedFile;

        }
        const settings = {
                ingestionMethod: method,
                pcapFilePath: filePath,
                FLOW_INACTIVITY_TIMEOUT_MS_TCP: parseInt(document.getElementById("tcpTimeout").value),
                FLOW_INACTIVITY_TIMEOUT_MS_UDP: parseInt(document.getElementById("udpTimeout").value),
                FLOW_INACTIVITY_TIMEOUT_MS_OTHER: parseInt(document.getElementById("otherTimeout").value),
                FLOW_MAX_AGE_MS_TCP: parseInt(document.getElementById("tcpMaxAge").value),
                FLOW_MAX_AGE_MS_UDP: parseInt(document.getElementById("udpMaxAge").value),
                FLOW_MAX_AGE_MS_OTHER: parseInt(document.getElementById("otherMaxAge").value),
        };
        console.log("[UPDATING]:", settings);

        await updateSettings(settings);
});

/**
 * This function fetches the current ingestion method from the server and updates the corresponding radio button.
 */
async function fetchIngestionMethod() {
        try {
                const response = await fetch("/api/getIngestionMethod");
                if (response.ok) {
                        const data = await response.json();
                        const method = data.ingestionMethod;
                        console.log("Fetched ingestion method :", method);
                        if (method) {
                                const radioToCheck = document.querySelector(`input[name="ingestionMethod"][value="${method}"]`);
                                if (radioToCheck) {
                                        radioToCheck.checked = true;
                                }
                        }
                } else {
                        console.error("Failed to fetch ingestion method");
                }
        } catch (err) {
                console.error("Error fetching ingestion method:", err);
        }
}


async function loadPcapFilesAndActive() {
        try {
                const response = await fetch("/api/pcapInfo");
                const data = await response.json();

                const pcapSelect = document.getElementById("pcapFile");
                pcapSelect.innerHTML = "";

                // Ajouter les fichiers
                if (data.files && data.files.length > 0) {
                        data.files.forEach(file => {
                                const option = document.createElement("option");
                                option.value = file;
                                option.textContent = file;
                                pcapSelect.appendChild(option);
                        });
                } else {
                        const option = document.createElement("option");
                        option.textContent = "Aucun fichier trouvé";
                        option.disabled = true;
                        pcapSelect.appendChild(option);
                }

                // Sélectionner le fichier actif
                if (data.activePcapFile) {
                        const optionToSelect = Array.from(pcapSelect.options)
                                .find(option => option.value === data.activePcapFile);
                        if (optionToSelect) optionToSelect.selected = true;
                }

                console.log("Loaded PCAP files and active file:", data);

        } catch (err) {
                console.error("Error loading PCAP files and active file:", err);
        }
}


/**
 * This function updates the visibility of the PCAP file select based on the selected ingestion method.
 */
async function updatePcapAndInterfacesVisibility() {
        const pcapSelectContainer = document.getElementById("pcapSelectContainer");
        const realtimeSelectContainer = document.getElementById("realtimeSelectContainer");
        const selected = document.querySelector('input[name="ingestionMethod"]:checked');

        if (selected && selected.value === "pcap") {
                realtimeSelectContainer.style.display = "none";
                pcapSelectContainer.style.display = "block";
                // await loadPcapFiles();
                // await loadActivePcapFile();
                loadPcapFilesAndActive();
        } else if (selected && selected.value === "realtime") {
                pcapSelectContainer.style.display = "none";
                realtimeSelectContainer.style.display = "block";
                loadNetworkInterfacesAndActive();
        }

        console.log("Updated PCAP and Network Interface visibility based on ingestion method");
        console.log("Current selected ingestion method:", selected ? selected.value : "none");
}

/**
 * Récupère les interfaces réseau depuis le backend et remplit le <select>
 */
async function loadNetworkInterfacesAndActive() {
        try {
                const response = await fetch("/api/networkInfo");
                const data = await response.json();

                const select = document.getElementById("realtimeInterface");
                select.innerHTML = "";

                // Ajouter les interfaces
                if (data.interfaces && data.interfaces.length > 0) {
                        data.interfaces.forEach(iface => {
                                const option = document.createElement("option");
                                option.value = iface;
                                option.textContent = iface;
                                select.appendChild(option);
                        });
                } else {
                        const option = document.createElement("option");
                        option.textContent = "Aucune interface trouvée";
                        option.disabled = true;
                        select.appendChild(option);
                }

                // Sélectionner l’interface active
                if (data.activeInterface) {
                        const optionToSelect = Array.from(select.options)
                                .find(o => o.value === data.activeInterface);
                        if (optionToSelect) optionToSelect.selected = true;
                }

                console.log("Loaded network interfaces and active interface:", data);

        } catch (err) {
                console.error("Error loading network interfaces and active interface:", err);
        }
}



/**
 * This function handles the reset of the dashboard, clearing all data and resetting the input fields.
 */
document.addEventListener('DOMContentLoaded', async () => {
        await fetchIngestionMethod();

        const ingestionRadios = document.querySelectorAll('input[name="ingestionMethod"]');

        // Add change event listeners to ingestion method radios
        ingestionRadios.forEach(radio => {
                radio.addEventListener("change", updatePcapAndInterfacesVisibility);
        });

        // Initial update on page load
        await updatePcapAndInterfacesVisibility();
});