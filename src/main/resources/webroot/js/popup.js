export const modal = document.getElementById("flowModal");
export const modalContent = document.getElementById("jsonContent");
export const closeBtn = document.getElementById("closeModal");
export const modalTitle = document.getElementById("modalTitle");


export function openModal(title, content, type) {
        modalTitle.textContent = title;
        modal.style.display = "block";
        if (type === "json") {
                modalContent.textContent = JSON.stringify(content, null, 2);
        } else {
                modalContent.textContent = content;
        }
}

export function closeModal() {
        modal.style.display = "none";
}

closeBtn.addEventListener("click", closeModal);