const API_BASE_URL = "http://localhost:5000";

async function loadMetrics() {
    try {
        const response = await fetch(`${API_BASE_URL}/get_metrics`);
        const data = await response.json();

        document.getElementById("orders").textContent = data.orders;
        document.getElementById("revenue").textContent = `$${data.revenue}`;
        document.getElementById("products").textContent = data.products;
        document.getElementById("branches").textContent = data.branches;

        document.getElementById("last-updated").textContent =
            `Last updated: ${data.last_updated}`;

    } catch (error) {
        console.error("Error loading metrics:", error);
    }
}

let salesChart = null;

async function loadSalesChart() {
    try {
        const response = await fetch(`${API_BASE_URL}/sales_branch`);
        const data = await response.json();

        const labels = data.map(item => item.branch);
        const values = data.map(item => item.sales);

        const ctx = document.getElementById("salesChart").getContext("2d");

        if (salesChart) {
            salesChart.destroy();
        }

        salesChart = new Chart(ctx, {
            type: "doughnut",
            data: {
                labels: labels,
                datasets: [{
                    data: values,
                    backgroundColor: [
                        "#0f766e",
                        "#0ea5e9",
                        "#6366f1",
                        "#f59e0b",
                        "#ef4444",
                        "#8b5cf6"
                    ],
                    borderWidth: 2,
                    borderColor: "#ffffff"
                }]
            },
            options: {
                responsive: true,
                cutout: "60%",
                animation: {
                    duration: 0
                },
                plugins: {
                    legend: {
                        position: "bottom",
                        labels: {
                            padding: 16,
                            font: { size: 13 }
                        }
                    }
                }
            }
        });

    } catch (error) {
        console.error("Error loading sales chart:", error);
    }
}
let paymentChart = null;

async function loadPaymentChart() {
    try {
        const response = await fetch(`${API_BASE_URL}/sales_by_payment`);
        const data = await response.json();

        const labels = data.map(item => item.payment_method);
        const values = data.map(item => item.sales);

        const ctx = document.getElementById("paymentChart").getContext("2d");

        if (paymentChart) {
            paymentChart.destroy();
        }

        paymentChart = new Chart(ctx, {
            type: "doughnut",
            data: {
                labels: labels,
                datasets: [{
                    data: values,
                    backgroundColor: [
                        "#36A2EB",
                        "#4BC0C0",
                        "#FF6384",
                        "#FFCE56",
                        "#9966FF",
                        "#FF9F40"
                    ],
                    borderColor: "#ffffff",
                    borderWidth: 2
                }]
            },
             options: {
                responsive: true,
                cutout: "60%",
                maintainAspectRatio: false,
                animation: {
                    duration: 0
                },
                plugins: {
                    legend: {
                        position: "bottom",
                        labels: {
                            boxWidth: 40,
                            padding: 15,
                            font: {
                                size: 13
                            }
                        }
                    }
                },
                layout: {
                    padding: 20
                },
                radius: "105%"
            }
        });

    } catch (error) {
        console.error("Error loading payment chart:", error);
    }
}
async function loadBranchPerformance() {
    try {
        const response = await fetch(`${API_BASE_URL}/branch_performance`);
        const data = await response.json();

        const table = document.getElementById("branch-table");
        table.innerHTML = "";

        data.forEach(branch => {
            table.innerHTML += `
                <tr>
                    <td>
                        <span class="branch-badge">
                            ${branch.branch}
                        </span>
                     </td>
                    <td>${branch.orders}</td>
                    <td>
                        <span class="revenue-badge">
                            $${branch.revenue}
                         </span>
                    </td>
                    <td>
                        <span class="products-badge">
                            ${branch.products}
                        </span>
                    </td>
                    <td>$${branch.avg_order}</td>
                 </tr>
`;
        });

    } catch (error) {
        console.error("Error loading branch performance:", error);
    }
}


function loadAll() {
    loadMetrics();
    loadSalesChart();
    loadLatestOrders();
    loadBranchPerformance();
    loadPaymentChart();
}

loadAll();                    // First time loading, the moment the page opens.
setInterval(loadAll, 5000);  // Then repeat every 5 seconds 5000 milliseconds.


async function loadBranchPerformance() {
    try {
        const response = await fetch(`${API_BASE_URL}/branch_performance`);
        const data = await response.json();

        const table = document.getElementById("branch-table");
        table.innerHTML = "";

        data.forEach(branch => {
            table.innerHTML += `
                <tr>
                    <td>
                        <span class="branch-badge">
                            ${branch.branch}
                        </span>
                     </td>
                    <td>${branch.orders}</td>
                    <td>
                        <span class="revenue-badge">
                            $${branch.revenue}
                         </span>
                    </td>
                    <td>
                        <span class="products-badge">
                            ${branch.products}
                        </span>
                    </td>
                    <td>$${branch.avg_order}</td>
                 </tr>
`;
        });

    } catch (error) {
        console.error("Error loading branch performance:", error);
    }
}


async function loadBranchPerformance() {
    try {
        const response = await fetch(`${API_BASE_URL}/branch_performance`);
        const data = await response.json();

        const table = document.getElementById("branch-table");
        table.innerHTML = "";

        data.forEach(branch => {
            table.innerHTML += `
                <tr>
                    <td>
                        <span class="branch-badge">
                            ${branch.branch}
                        </span>
                     </td>
                    <td>${branch.orders}</td>
                    <td>
                        <span class="revenue-badge">
                            $${branch.revenue}
                         </span>
                    </td>
                    <td>
                        <span class="products-badge">
                            ${branch.products}
                        </span>
                    </td>
                    <td>$${branch.avg_order}</td>
                 </tr>
`;
        });

    } catch (error) {
        console.error("Error loading branch performance:", error);
    }
}

async function loadLatestOrders() {
    try {

        const response = await fetch(`${API_BASE_URL}/latest_orders`);
        const data = await response.json();

        const container = document.getElementById("latest-orders");
        container.innerHTML = "";

       data.slice(0, 5).forEach(order => {

            const items = order.items
                .map(item => `• ${item.product_name} x${item.quantity}`)
                .join("<br>");

            container.innerHTML += `
                <div class="order-card">

                    <div class="order-header">
                        <strong>${order.customer_name}</strong>
                        <span class="branch-badge">${order.branch}</span>
                    </div>

                    <div class="order-payment">
                        💳 ${order.payment_method}
                    </div>

                    <div class="order-items">
                        ${items}
                    </div>

                    <div class="order-notes">
                        📝 ${order.notes}
                    </div>

                </div>
            `;

        });

    } catch (error) {
        console.error("Error loading latest orders:", error);
    }
}

function refreshAll() {
    loadMetrics();
    loadSalesChart();
    loadPaymentChart();
    loadLatestOrders();
    loadBranchPerformance();
}

refreshAll();
setInterval(refreshAll, 1000);
