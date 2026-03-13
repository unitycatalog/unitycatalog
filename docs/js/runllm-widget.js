document.addEventListener("DOMContentLoaded", function() {
    var script = document.createElement("script");
    script.defer = true;
    script.type = "module";
    script.id = "runllm-widget-script";
    script.src = "https://widget.runllm.com";
    script.setAttribute("runllm-preset", "mkdocs");
    script.setAttribute("runllm-server-address", "https://api.runllm.com");
    script.setAttribute("runllm-keyboard-shortcut", "Mod+j");
    script.setAttribute("runllm-name", "Unity Catalog");
    script.setAttribute("runllm-position", "BOTTOM_RIGHT");
    script.setAttribute("runllm-assistant-id", "269");
    script.setAttribute("runllm-theme-color", "#008ED9");
    script.setAttribute("runllm-brand-logo", "https://docs.unitycatalog.io/assets/images/uc-logo-mark-reverse.png");
    script.setAttribute("runllm-community-type", "slack");
    script.setAttribute("runllm-slack-community-url", "https://go.unitycatalog.io/slack");
    script.setAttribute("runllm-disable-ask-a-person", "true");


    document.head.appendChild(script);
});