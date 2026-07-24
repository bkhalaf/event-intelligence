const API_BASE_URL = "http://localhost:5000";

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

let reviewsSummary = {};

async function loadReviewsSummary() {
  try {
    const response = await fetch(`${API_BASE_URL}/product_reviews_summary`);
    const data = await response.json();

    reviewsSummary = {};
    data.forEach((entry) => {
      reviewsSummary[entry.product_id] = entry;
    });
  } catch (error) {
    console.error("Error loading product reviews summary:", error);
  } finally {
    renderProducts();
  }
}

function renderRatingSummary(insights) {
  if (!insights || insights.review_count === 0) {
    return `
      <span class="stars stars--placeholder">☆☆☆☆☆</span>
      <span class="rating-value">No reviews yet</span>
    `;
  }

  const fullStars = Math.round(insights.avg_rating);
  const stars = "★".repeat(fullStars) + "☆".repeat(5 - fullStars);

  return `
    <span class="stars">${stars}</span>
    <span class="rating-value">${insights.avg_rating.toFixed(1)} (${insights.review_count} review${insights.review_count === 1 ? "" : "s"})</span>
  `;
}

function renderRecentReviews(insights) {
  if (!insights || insights.recent_reviews.length === 0) {
    return `<div class="product-reviews">No reviews yet. Be the first to leave one!</div>`;
  }

  const items = insights.recent_reviews
    .map((review) => {
      const stars = "★".repeat(review.rating) + "☆".repeat(5 - review.rating);
      const text = review.review_text ? review.review_text : "<em>No comment</em>";
      return `
        <div class="review-item">
          <div class="review-item__header">
            <span class="review-item__name">${review.customer_name}</span>
            <span class="review-item__stars">${stars}</span>
          </div>
          <div class="review-item__text">${text}</div>
        </div>
      `;
    })
    .join("");

  return `<div class="product-reviews">${items}</div>`;
}

function renderReviewForm(product) {
  return `
    <form class="review-form" data-product-id="${product.product_id}" data-rating="0">
      <div class="review-form__stars" data-role="star-picker">
        ${[1, 2, 3, 4, 5]
          .map(
            (value) => `<button type="button" class="star-btn" data-value="${value}" aria-label="${value} star">☆</button>`
          )
          .join("")}
      </div>
      <input
        type="text"
        class="review-form__name"
        placeholder="Your name (optional)"
        maxlength="60"
      />
      <textarea
        class="review-form__text"
        placeholder="Share your thoughts about this product..."
        rows="2"
        maxlength="500"
      ></textarea>
      <button type="submit" class="review-form__submit">Submit Review</button>
      <div class="review-form__status"></div>
    </form>
  `;
}

function renderProducts() {
  productsList.innerHTML = products
    .map((product) => {
      const insights = reviewsSummary[product.product_id];

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
            ${renderRatingSummary(insights)}
          </div>
          ${renderRecentReviews(insights)}
          ${renderReviewForm(product)}
        </div>
      `;
    })
    .join("");
}

productsList.addEventListener("click", (event) => {
  const starButton = event.target.closest(".star-btn");
  if (!starButton) return;

  const form = starButton.closest(".review-form");
  const value = Number(starButton.dataset.value);
  form.dataset.rating = value;

  form.querySelectorAll(".star-btn").forEach((btn) => {
    const isActive = Number(btn.dataset.value) <= value;
    btn.textContent = isActive ? "★" : "☆";
    btn.classList.toggle("is-active", isActive);
  });
});

productsList.addEventListener("submit", async (event) => {
  const form = event.target.closest(".review-form");
  if (!form) return;

  event.preventDefault();

  const statusBox = form.querySelector(".review-form__status");
  const rating = Number(form.dataset.rating);
  const productId = form.dataset.productId;
  const product = products.find((p) => p.product_id === productId);
  const customerName = form.querySelector(".review-form__name").value.trim();
  const reviewText = form.querySelector(".review-form__text").value.trim();

  if (!rating) {
    statusBox.className = "review-form__status error";
    statusBox.textContent = "Please select a star rating.";
    return;
  }

  statusBox.className = "review-form__status";
  statusBox.textContent = "Submitting review...";

  try {
    const response = await fetch(`${API_BASE_URL}/review`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        product_id: product.product_id,
        product_name: product.product_name,
        customer_name: customerName,
        rating,
        review_text: reviewText
      })
    });

    const data = await response.json();

    if (!response.ok) {
      throw new Error(data.detail || "Failed to send review.");
    }

    statusBox.className = "review-form__status success";
    statusBox.textContent = "Thanks! Your review has been submitted.";
    form.reset();
    form.dataset.rating = "0";
    form.querySelectorAll(".star-btn").forEach((btn) => {
      btn.textContent = "☆";
      btn.classList.remove("is-active");
    });
  } catch (error) {
    statusBox.className = "review-form__status error";
    statusBox.textContent = error.message;
  }
});

renderProducts();
loadReviewsSummary();
