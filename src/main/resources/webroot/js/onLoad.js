//Set mode selected api getIngestionMethod
async function fetchIngestionMethod() {
        try {
                const response = await fetch("/api/getIngestionMethod");
                if (response.ok) {
                        const data = await response.json();
                        const method = data.ingestionMethod;
                        console.log("Fetched ingestion method:", method);
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

// Call the function on page load
window.addEventListener("load", fetchIngestionMethod);