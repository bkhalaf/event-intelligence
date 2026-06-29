const products = [
  {
    product_id: "E1001",
    product_name: "Lenovo IdeaPad Laptop",
    unit_price: 3200.0,
    image: "images/Lenovo_IdeaPad_Laptop.avif"
  },
  {
    product_id: "E1002",
    product_name: "Anker Power Bank",
    unit_price: 180.0,
    image: "images/Anker_Power_Bank.jpg"
  },
  {
    product_id: "E1003",
    product_name: "Wireless Headphones",
    unit_price: 250.0,
    image: "images/Wireless_Headphones.avif"
  },
  {
    product_id: "E1004",
    product_name: "Gaming Mouse",
    unit_price: 95.0,
    image: "images/Gaming_Mouse.avif"
  },
  {
    product_id: "E1005",
    product_name: "Mechanical Keyboard",
    unit_price: 210.0,
    image: "images/Mechanical_Keyboard.avif"
  },
  {
    product_id: "E1006",
    product_name: "Smart Watch",
    unit_price: 420.0,
    image: "images/Smart_Watch.avif"
  }
];

const productsList = document.getElementById("productsList");
const orderForm = document.getElementById("orderForm");
const orderPreview = document.getElementById("orderPreview");
const resultBox = document.getElementById("resultBox");
const resetBtn = document.getElementById("resetBtn");

function renderProducts() {
  productsList.innerHTML = products
    .map(
      (product) => `
        <div class="product-card">
          <img
            src="${product.image}"
            alt="${product.product_name}"
            class="product-image"
          />
          <h4>${product.product_name}</h4>
          <div class="product-meta">
            Product ID: ${product.product_id}<br />
            Unit Price: ${product.unit_price.toFixed(2)} ILS
          </div>
          <div class="product-controls">
            <label for="qty-${product.product_id}">Qty</label>
            <input
              type="number"
              id="qty-${product.product_id}"
              min="0"
              value="0"
              data-product-id="${product.product_id}"
            />
          </div>
        </div>
      `
    )
    .join("");
}

function getSelectedItems() {
  return products
    .map((product) => {
      const qtyInput = document.getElementById(`qty-${product.product_id}`);
      const quantity = Number(qtyInput.value) || 0;

      if (quantity > 0) {
        return {
          product_id: product.product_id,
          product_name: product.product_name,
          quantity,
          unit_price: product.unit_price
        };
      }

      return null;
    })
    .filter(Boolean);
}

function buildPayload() {
  return {
    branch: document.getElementById("branch").value,
    customer_name: document.getElementById("customer_name").value.trim(),
    payment_method: document.getElementById("payment_method").value,
    notes: document.getElementById("notes").value.trim(),
    items: getSelectedItems()
  };
}

function updatePreview() {
  const payload = buildPayload();
  const hasBasicData =
    payload.branch ||
    payload.customer_name ||
    payload.payment_method ||
    payload.notes ||
    payload.items.length > 0;

  if (!hasBasicData) {
    orderPreview.className = "preview empty";
    orderPreview.textContent = "Fill the form to preview the payload before sending.";
    return;
  }

  orderPreview.className = "preview";
  orderPreview.textContent = JSON.stringify(payload, null, 2);
}

async function submitOrder(event) {
  event.preventDefault();

  const payload = buildPayload();

  if (!payload.branch || !payload.customer_name || !payload.payment_method) {
    resultBox.className = "result-box error";
    resultBox.textContent = "Please fill in branch, customer name, and payment method.";
    return;
  }

  if (payload.items.length === 0) {
    resultBox.className = "result-box error";
    resultBox.textContent = "Please select at least one product with quantity more than 0.";
    return;
  }

  resultBox.className = "result-box";
  resultBox.textContent = "Sending order...";

  try {
    const response = await fetch("http://localhost:5000/order", {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify(payload)
    });

    const data = await response.json();

    if (!response.ok) {
      throw new Error(data.detail || "Failed to send order.");
    }

    resultBox.className = "result-box success";
    resultBox.textContent = JSON.stringify(data, null, 2);
  } catch (error) {
    resultBox.className = "result-box error";
    resultBox.textContent = error.message;
  }
}

function resetForm() {
  orderForm.reset();
  products.forEach((product) => {
    const qtyInput = document.getElementById(`qty-${product.product_id}`);
    if (qtyInput) qtyInput.value = 0;
  });

  orderPreview.className = "preview empty";
  orderPreview.textContent = "Fill the form to preview the payload before sending.";

  resultBox.className = "result-box empty";
  resultBox.textContent = "No request sent yet.";
}

renderProducts();
updatePreview();

orderForm.addEventListener("input", updatePreview);
orderForm.addEventListener("submit", submitOrder);
resetBtn.addEventListener("click", resetForm);