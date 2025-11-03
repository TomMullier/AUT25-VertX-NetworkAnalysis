// --- SETTINGS HANDLERS --- //
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
                        console.log("[✅ SETTINGS UPDATED]", settings);
                        console.log("✅ Settings updated successfully");
                } else {
                        console.log("❌ Failed to update settings");
                }
        } catch (err) {
                console.error("Error updating settings:", err);
                console.log("❌ Error updating settings");
        }
}

// Form submission handler
document.getElementById("SettingsForm").addEventListener("submit", (e) => {
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
        const settings = {
                ingestionMethod: method,
                FLOW_INACTIVITY_TIMEOUT_MS_TCP: parseInt(document.getElementById("tcpTimeout").value),
                FLOW_INACTIVITY_TIMEOUT_MS_UDP: parseInt(document.getElementById("udpTimeout").value),
                FLOW_INACTIVITY_TIMEOUT_MS_OTHER: parseInt(document.getElementById("otherTimeout").value),
                FLOW_MAX_AGE_MS_TCP: parseInt(document.getElementById("tcpMaxAge").value),
                FLOW_MAX_AGE_MS_UDP: parseInt(document.getElementById("udpMaxAge").value),
                FLOW_MAX_AGE_MS_OTHER: parseInt(document.getElementById("otherMaxAge").value),
        };
        console.log("Updating settings:", settings);

        updateSettings(settings);
});