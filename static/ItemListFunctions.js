
function updateHiddenInput() {
    const listItems = document.querySelectorAll('#ing-list span');
    const ingredients = Array.from(listItems).map(span => span.innerText);
    document.getElementById('hidden-q').value = ingredients.join(',');
}

function addIngredient() {
    const input = document.getElementById('ing-input');
    const list = document.getElementById('ing-list');
    const val = input.value.trim()

    if (val !== "") {
        const li = document.createElement('li');
        li.className = "ing-item";
        li.innerHTML = `
            <span>${val}</span>
            <button type="button" class="del-btn" onclick="this.parentElement.remove(); updateHiddenInput();">x</button>
        `;
        list.appendChild(li);
        input.value = "";
        updateHiddenInput();
    }
}

function restoreIngredients() {
    const hiddenInput = document.getElementById('hidden-q');
    const list = document.getElementById('ing-list');

    if (!hiddenInput || !list) return;

    const queryValue = hiddenInput.value.trim();
    if (queryValue !== "") {
        const ingredients = queryValue.split(',');
        ingredients.forEach(ingredient => {
            const trimmed = ingredient.trim();
            if (trimmed.length > 0) {
                const li = document.createElement('li');
                li.className = 'ing-item';
                li.innerHTML = `
                    <span>${trimmed}</span>
                    <button type="button" class="del-btn" onclick="this.parentElement.remove(); updateHiddenInput();">x</button>
                `;
                list.appendChild(li);
            }
        });
    }
}

window.onload = restoreIngredients;