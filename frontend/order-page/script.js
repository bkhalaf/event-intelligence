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
const cartItems = document.getElementById("cartItems");
const cartTotal = document.getElementById("cartTotal");
const resultBox = document.getElementById("resultBox");
const resetBtn = document.getElementById("resetBtn");

function renderProducts() {
  productsList.innerHTML = products
    .map(
      (product) => `
        <div class="product-card" id="card-${product.product_id}">
          <img
            src="${product.image}"
            alt="${product.product_name}"
            class="product-image"
          />
          <h4>${product.product_name}</h4>
          <div class="product-meta">Product ID: ${product.product_id}</div>
          <div class="product-controls">
            <span class="product-price">${product.unit_price.toFixed(2)} ILS</span>
            <div class="stepper">
              <button type="button" data-action="dec" data-product-id="${product.product_id}" aria-label="Decrease quantity">&minus;</button>
              <input
                type="number"
                id="qty-${product.product_id}"
                min="0"
                value="0"
                readonly
                data-product-id="${product.product_id}"
              />
              <button type="button" data-action="inc" data-product-id="${product.product_id}" aria-label="Increase quantity">+</button>
            </div>
          </div>
        </div>
      `
    )
    .join("");
}

function setQuantity(productId, quantity) {
  const qtyInput = document.getElementById(`qty-${productId}`);
  const card = document.getElementById(`card-${productId}`);
  const value = Math.max(0, quantity);
  qtyInput.value = value;
  card.classList.toggle("is-selected", value > 0);
  updateCart();
}

productsList.addEventListener("click", (event) => {
  const button = event.target.closest("button[data-action]");
  if (!button) return;

  const productId = button.dataset.productId;
  const qtyInput = document.getElementById(`qty-${productId}`);
  const current = Number(qtyInput.value) || 0;
  const next = button.dataset.action === "inc" ? current + 1 : current - 1;
  setQuantity(productId, next);
});

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

function updateCart() {
  const items = getSelectedItems();

  if (items.length === 0) {
    cartItems.className = "cart__items empty";
    cartItems.textContent = "No items selected yet.";
    cartTotal.textContent = "0.00 ILS";
    return;
  }

  cartItems.className = "cart__items";
  cartItems.innerHTML = items
    .map(
      (item) => `
        <div class="cart-row">
          <div>
            <div class="cart-row__name">${item.product_name}</div>
            <div class="cart-row__qty">Qty ${item.quantity} &times; ${item.unit_price.toFixed(2)} ILS</div>
          </div>
          <div class="cart-row__price">${(item.quantity * item.unit_price).toFixed(2)} ILS</div>
        </div>
      `
    )
    .join("");

  const total = items.reduce((sum, item) => sum + item.quantity * item.unit_price, 0);
  cartTotal.textContent = `${total.toFixed(2)} ILS`;
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
    setQuantity(product.product_id, 0);
  });

  resultBox.className = "result-box empty";
  resultBox.textContent = "No request sent yet.";
}

renderProducts();
updateCart();

orderForm.addEventListener("submit", submitOrder);
resetBtn.addEventListener("click", resetForm);
