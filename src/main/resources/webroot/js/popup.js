export const modal = document.getElementById("flowModal");
export const modalBox = modal.querySelector(".bg-white"); // ta boîte blanche
export const modalContent = document.getElementById("jsonContent");
export const closeBtn = document.getElementById("closeModal");
export const modalTitle = document.getElementById("modalTitle");

// Fonction pour ouvrir la modale
export function openModal(title, content, type) {
        modalTitle.textContent = title;

        if (type === "json") {
                modalContent.textContent = JSON.stringify(content, null, 2);
        } else {
                modalContent.textContent = content;
        }

        // On affiche la modale avant d’appliquer la transition
        modal.classList.remove("hidden");
        modal.classList.remove("opacity-0");
        modal.classList.add("flex", "opacity-100");
        modalBox.classList.remove("scale-95");
        modalBox.classList.add("scale-100");
}

// Fonction pour fermer la modale
export function closeModal() {
        modal.classList.remove("opacity-100");
        modal.classList.add("opacity-0");
        modalBox.classList.remove("scale-100");
        modalBox.classList.add("scale-95");

        // Après l'animation, on la cache complètement
        setTimeout(() => {
                modal.classList.add("hidden");
                modal.classList.remove("flex");
        }, 300);
}

// Écouteurs
closeBtn.addEventListener("click", closeModal);
modal.addEventListener("click", (e) => {
        if (e.target === modal) closeModal();
});