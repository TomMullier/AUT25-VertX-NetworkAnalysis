const ws = new WebSocket(`ws://${location.host}/`);

const statusEl = document.getElementById("connectionStatus");
const messagesEl = document.getElementById("messageContainer");

ws.onopen = () => {
        statusEl.textContent = "✅ Connecté au serveur WebSocket";
        statusEl.classList.add("text-green-600");
};

ws.onmessage = (event) => {
        try {
                const data = JSON.parse(event.data);
                const div = document.createElement("div");
                div.textContent = JSON.stringify(data, null, 2);
                div.classList.add("border-b", "border-gray-300", "py-1");
                messagesEl.prepend(div);
        } catch {
                console.warn("Message non JSON reçu :", event.data);
        }
};

ws.onerror = (err) => {
        statusEl.textContent = "❌ Erreur WebSocket : " + err.message;
        statusEl.classList.add("text-red-600");
};

ws.onclose = () => {
        statusEl.textContent = "🔴 Déconnecté du serveur WebSocket";
        statusEl.classList.add("text-red-600");
};