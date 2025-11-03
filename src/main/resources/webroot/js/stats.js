// Data storage for charts
export const severityCount = {
        NoRisk: 0,
        Low: 0,
        Medium: 0,
        Severe: 0,
        High: 0,
        Critical: 0,
        Emergency: 0,
};
export const riskCount = {

};

let severityChart, riskChart;

// Graph initialization
export function initCharts() {
        const ctx1 = document.getElementById("severityChart").getContext("2d");
        const ctx2 = document.getElementById("riskChart").getContext("2d");

        severityChart = new Chart(ctx1, {
                type: "doughnut",
                data: {
                        labels: Object.keys(severityCount),
                        datasets: [{
                                data: Object.values(severityCount),
                                backgroundColor: [
                                        "#B0BEC5", // NoRisk (text-gray-400)
                                        "#81C784", // Low (text-green-400)
                                        "#FFEB3B", // Medium (text-yellow-400)
                                        "#FF9800", // High (text-orange-400)
                                        "#F44336", // Severe (text-red-400)
                                        "#b61c1c", // Critical (text-red-700)
                                        "#640000" // Emergency (text-red-900)
                                ]
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

        riskChart = new Chart(ctx2, {
                type: "bar",
                data: {
                        labels: Object.keys(riskCount),
                        datasets: [{
                                data: Object.values(riskCount),
                                backgroundColor: Array.from({ length: 50 }, (_, i) => `hsl(${(i * 360) / 50}, 100%, 50%)`)
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
}

// Update charts with new data
export function updateCharts() {
        severityChart.data.datasets[0].data = Object.values(severityCount);
        riskChart.data.labels = Object.keys(riskCount);
        riskChart.data.datasets[0].data = Object.values(riskCount);

        severityChart.update();
        riskChart.update();
}


// Increment count utility
export function incrementCount(container, key) {
        if (!container[key]) {
                container[key] = 1;
        } else {
                container[key]++;
        }
}