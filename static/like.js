
function updateLike(event) {
    if (event) event.preventDefault();
    const btn = document.getElementById('like-btn');
    if (!btn) return;
    const recipeId = btn.getAttribute('data-recipe-id');

    fetch(`/like/${recipeId}`, {
        method: 'POST',
    })
    .then(response => {
        if (!response.ok) throw new Error('Network response was not ok');
        return response.json()
    })
    .then(data => {
        const isLiked = (data.status == "liked")
        btn.classList.toggle('is-liked', isLiked);
        const heartSpan = btn.querySelector('.heart-detail');
        const textSpan = btn.querySelector('.text');
        if (heartSpan) {
            heartSpan.textContent = isLiked ? '❤️' : '🤍';
        }
        if (textSpan) {
            textSpan.textContent = isLiked ? 'Liked' : 'Like';
        }
    })
    .catch(err => console.error('Error liking recipe:', err));
}