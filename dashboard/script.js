const API_BASE_URL = "http://localhost:5000";

async function loadMetrics() {
    try {
        const response = await fetch(`${API_BASE_URL}/get_metrics`);
        const data = await response.json();

        document.getElementById("orders").textContent =
        Number(data.orders).toLocaleString();

        document.getElementById("revenue").textContent =
        ` $${Number(data.revenue).toLocaleString()}`;

        document.getElementById("products").textContent =
        Number(data.products).toLocaleString();

        document.getElementById("branches").textContent =
        Number(data.branches).toLocaleString();

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
            type: "pie",
            data: {
                labels: labels,
                datasets: [{
                    data: values,
                    backgroundColor: [
                        "#4F46E5",
                        "#10B981",
                        "#F59E0B",
                        "#EF4444",
                        "#8B5CF6",
                        "#06B6D4"
                    ],
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                plugins: {
                    legend: {
                        position: "bottom"
                    }
                }
            }
        });

    } catch (error) {
        console.error("Error loading sales chart:", error);
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
                        <span>${order.branch}</span>
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
loadMetrics();
loadSalesChart();
loadLatestOrders();
loadBranchPerformance();