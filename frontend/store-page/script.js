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

// TODO: replace with a real call once the reviews endpoint exists, e.g.
// GET `${API_BASE_URL}/products/${productId}/reviews_summary` -> { rating, summary }
function getProductInsights(product) {
  return {
    rating: null,
    reviewsSummary: "Reviews summary will appear here once the reviews endpoint is connected."
  };
}

function renderStars(rating) {
  if (rating === null || rating === undefined) {
    return `<span class="stars stars--placeholder">☆☆☆☆☆</span><span class="rating-value">No rating yet</span>`;
  }

  const fullStars = Math.round(rating);
  const stars = "★".repeat(fullStars) + "☆".repeat(5 - fullStars);
  return `<span class="stars">${stars}</span><span class="rating-value">${rating.toFixed(1)}</span>`;
}

function renderProducts() {
  productsList.innerHTML = products
    .map((product) => {
      const insights = getProductInsights(product);

      return `
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
          <div class="product-rating">
            ${renderStars(insights.rating)}
          </div>
          <div class="product-reviews">
            ${insights.reviewsSummary}
          </div>
        </div>
      `;
    })
    .join("");
}

renderProducts();
