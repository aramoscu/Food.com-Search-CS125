
function updateLike(event) {
    if (event) event.preventDefault();
    const btn = document.getElementById('like-btn');
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
        btn.querySelector('.heart').innerText = isLiked ? '❤️' : '🤍';
        btn.querySelector('.text').innerText = isLiked ? 'Liked' : 'Like';
    })
    .catch(err => console.error('Error liking recipe:', err));
}