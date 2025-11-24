// === Data storage for charts ===
export const severityCount = {
        NoRisk: 0,
        Low: 0,
        Medium: 0,
        Severe: 0,
        High: 0,
        Critical: 0,
        Emergency: 0,
};

const colors = {
        Emergency: "#640000",
        Critical: "#b61c1c",
        High: "#FF9800",
        Severe: "#FFEB3B",
        Medium: "#81C784",
        Low: "#2196F3",
        NoRisk: "#B0BEC5"
};

export const riskCount = {};

export const protocolCount = {};
export const countryLinks = {};

export const flowDelays = {}; // clé = flowKey, valeur = liste des delays
let treatmentDelaysChart; // instance Chart.js


// === Chart instances ===

let severityChart, riskChart, protocolChart;

export let trafficMap;

export function initMap() {
        trafficMap = L.map('trafficMap').setView([20, 0], 2); // centrée sur le globe

        // Ajouter le fond de carte
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
                attribution: '&copy; OpenStreetMap contributors'
        }).addTo(trafficMap);
}


// === Graph initialization ===
export function initCharts() {
        const ctx1 = document.getElementById("severityChart").getContext("2d");
        const ctx2 = document.getElementById("riskChart").getContext("2d");
        const ctx3 = document.getElementById("protocolChart").getContext("2d"); // --- 4️⃣ Treatment delays per flow ---
        const ctx4 = document.getElementById("treatmentDelaysChart").getContext("2d");
        treatmentDelaysChart = new Chart(ctx4, {
                type: "line",
                data: {
                        labels: [], // flowKey ou timestamp
                        datasets: [{
                                label: "Average Packet Treatment Delay (ms)",
                                data: [],
                                borderColor: "#2563eb",
                                backgroundColor: "#2563eb",
                                fill: false,
                                tension: 0.3,
                                pointRadius: 3,
                                pointHoverRadius: 6,
                                borderWidth: 2
                        }]
                },
                options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        animation: {
                                duration: 500
                        },
                        scales: {
                                x: {
                                        title: {
                                                display: false,
                                                text: "Flow"
                                        },
                                        ticks: {
                                                display: false
                                        }


                                },
                                y: {
                                        title: {
                                                display: true,
                                                text: "Avg Delay (ms)"
                                        },
                                        beginAtZero: true
                                }
                        }
                }
        });


        // --- 1️⃣ Severities ---
        severityChart = new Chart(ctx1, {
                type: "doughnut",
                data: {
                        labels: Object.keys(severityCount),
                        datasets: [{
                                data: Object.values(severityCount),
                                backgroundColor: Object.keys(severityCount).map(key => colors[key])
                        }]
                },
                options: {
                        plugins: {
                                legend: {
                                        position: "bottom"
                                },
                                title: {
                                        display: true,
                                        text: "Severities of Detected Risks"
                                }
                        }
                }
        });

        // --- 2️⃣ Risk types ---
        riskChart = new Chart(ctx2, {
                type: "bar",
                data: {
                        labels: Object.keys(riskCount),
                        datasets: [{
                                data: Object.values(riskCount),
                                backgroundColor: Array.from({
                                        length: 50
                                }, (_, i) => `hsl(${(i * 360) / 50}, 100%, 50%)`)
                        }]
                },
                options: {
                        plugins: {
                                legend: {
                                        display: false
                                },
                                title: {
                                        display: true,
                                        text: "Types of Detected Risks"
                                }
                        },
                        scales: {
                                x: {
                                        title: {
                                                display: true,
                                                text: "Types of Detected Risks"
                                        }
                                },
                                y: {
                                        title: {
                                                display: true,
                                                text: "Number of Occurrences"
                                        },
                                        beginAtZero: true
                                }
                        }
                }
        });

        // --- 3️⃣ Protocol distribution ---
        protocolChart = new Chart(ctx3, {
                type: "pie",
                data: {
                        labels: Object.keys(protocolCount),
                        datasets: [{
                                data: Object.values(protocolCount),
                                backgroundColor: Array.from({
                                        length: 20
                                }, (_, i) => `hsl(${(i * 360) / 20}, 80%, 60%)`)
                        }]
                },
                options: {
                        plugins: {
                                title: {
                                        display: true,
                                        text: "Protocol Distribution"
                                },
                                legend: {
                                        position: "bottom"
                                }
                        }
                }
        });

}





// === Increment utilities ===
export function incrementCount(container, key) {
        if (!container[key]) {
                container[key] = 1;
        } else {
                container[key]++;
        }
}

// === Increment link between countries ===
export function addCountryLink(srcCountry, dstCountry) {
        const key = `${srcCountry} -> ${dstCountry}`;
        incrementCount(countryLinks, key);
}
// === Update charts ===
export function updateCharts() {
        if (severityChart) severityChart.data.datasets[0].data = Object.values(severityCount);
        if (riskChart) {
                riskChart.data.labels = Object.keys(riskCount);
                riskChart.data.datasets[0].data = Object.values(riskCount);
        }
        if (protocolChart) {
                protocolChart.data.labels = Object.keys(protocolCount);
                protocolChart.data.datasets[0].data = Object.values(protocolCount);
        }

        // Update charts existants
        if (severityChart) severityChart.update();
        if (riskChart) riskChart.update();
        if (protocolChart) protocolChart.update();

}

let linkLayers = [];
const countryCoords = {};
const unknownFlows = {}; // Pour stocker les flux vers IP privées ou pays inconnus

export async function fetchCountryCoords() {
        try {
                const response = await fetch('https://restcountries.com/v3.1/all?fields=name,latlng');
                if (!response.ok) throw new Error('Failed to fetch countries');
                const countries = await response.json();

                if (Array.isArray(countries)) {
                        countries.forEach(country => {
                                if (country.name && country.name.common && Array.isArray(country.latlng)) {
                                        countryCoords[country.name.common] = country.latlng;
                                }
                        });
                } else {
                        console.error('Unexpected countries format', countries);
                }
        } catch (err) {
                console.error('Error fetching country coordinates:', err);
        }
}



export function updateMapLinks() {
        if (!trafficMap) return;

        // Supprime les anciens arcs / points
        linkLayers.forEach(layer => trafficMap.removeLayer(layer));
        linkLayers = [];
        Object.keys(unknownFlows).forEach(key => delete unknownFlows[key]);

        Object.entries(countryLinks).forEach(([pair, count]) => {
                const [src, dst] = pair.split(" -> ");
                const srcCoord = countryCoords[src];
                const dstCoord = countryCoords[dst];

                if (srcCoord && dstCoord) {
                        // --- Ligne entre deux pays connus ---
                        const line = L.polyline([srcCoord, dstCoord], {
                                color: 'red',
                                weight: Math.min(10, Math.sqrt(count) * 2),
                                opacity: 0.7
                        }).addTo(trafficMap);
                        linkLayers.push(line);
                } else if (srcCoord || dstCoord) {
                        // --- Au moins un pays inconnu ou privé, on fait un point ---
                        // Choisir un point existant connu si possible
                        const coord = srcCoord || dstCoord;

                        // Accumule le nombre de flux sur cette coordonnée
                        const keyCoord = coord.join(',');
                        unknownFlows[keyCoord] = (unknownFlows[keyCoord] || 0) + count;
                }
        });

        // Dessiner les points pour les flux inconnus / privés
        Object.entries(unknownFlows).forEach(([coordStr, count]) => {
                const coord = coordStr.split(',').map(Number);
                const circle = L.circleMarker(coord, {
                        radius: Math.min(20, Math.sqrt(count) * 3), // rayon proportionnel au nombre de flux
                        color: 'blue',
                        fillColor: 'blue',
                        fillOpacity: 0.5
                }).addTo(trafficMap);
                linkLayers.push(circle);
        });
}

export function updateFlowDelay(flowKey, delays) {
        // calcul de la moyenne
        const avgDelay = delays.reduce((a, b) => a + b, 0) / delays.length;

        // stocker la moyenne
        flowDelays[flowKey] = avgDelay;

        // mettre à jour le graphique
        const data = treatmentDelaysChart.data;
        if (!data.labels.includes(flowKey)) {
                data.labels.push(flowKey);
                data.datasets[0].data.push(avgDelay);
        } else {
                const index = data.labels.indexOf(flowKey);
                data.datasets[0].data[index] = avgDelay;
        }

        // scrolling automatique : conserver max 50 flows visibles
        const MAX_POINTS = 50;
        while (data.labels.length > MAX_POINTS) {
                data.labels.shift();
                data.datasets[0].data.shift();
        }

        treatmentDelaysChart.update();

        // Mettre à jour la valeur moyenne sur 50 flows
        const totalDelay = data.datasets[0].data.reduce((a, b) => a + b, 0);
        const averageDelay = (data.datasets[0].data.length > 0) ? (totalDelay / data.datasets[0].data.length) : 0;
        document.getElementById("averageDelayValue").innerText = averageDelay.toFixed(2);
}