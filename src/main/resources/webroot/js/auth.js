// LOGIN
const loginForm = document.getElementById("loginForm");

if (loginForm) {
    loginForm.addEventListener("submit", async (e) => {
        e.preventDefault();
        
        const email = document.getElementById("loginEmail").value;
        const password = document.getElementById("loginPassword").value;

        console.log("Login clicked");
        
        const res = await fetch("/login", {
            method: "POST",
            headers: {"Content-Type": "application/json"},
            body: JSON.stringify({ email, password })
        });
        
        const data = await res.json();
        
        if (data.success) {
            // sessionStorage.setItem("loggedIn", "true");
            // sessionStorage.setItem("name", data.name);// changed by me

            // ====== added by me ======
            localStorage.setItem("loggedIn", "true");
            localStorage.setItem("name", data.name);
            // ===============
            window.location.href = "/index.html";
        } else {
            alert("Invalid credentials");
        }
    });
}

// SIGNUP
const signupForm = document.getElementById("signupForm");

if (signupForm) {
    signupForm.addEventListener("submit", async (e) => {
        e.preventDefault();

    const name = document.getElementById("signupName").value;
    const email = document.getElementById("signupEmail").value;
    const password = document.getElementById("signupPassword").value;

    const res = await fetch("/signup", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name, email, password })
    });

    const data = await res.json();

    if (data.success) {
        alert("Account created");
        window.location.href = "/login.html";
    } else {
        alert(data.error);
    }
});}