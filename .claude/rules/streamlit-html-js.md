# Streamlit HTML + Client-Side JS Pattern

## Injecting JS into the parent page

Streamlit has two HTML rendering methods with very different behavior:

| Method | Script execution | DOM scope | Use for |
|--------|-----------------|-----------|---------|
| `st.markdown(html, unsafe_allow_html=True)` | **Strips `<script>` tags** | Parent page | Static HTML only |
| `st.html(html, unsafe_allow_javascript=True)` | **Executes scripts** | Parent page (NOT iframe) | JS that needs to manipulate parent DOM |

### Correct pattern: render HTML table + attach JS sort/interactions

```python
# 1. Render table with st.markdown (leverages parent CSS theme vars)
st.markdown('<table id="my-tbl">...</table>', unsafe_allow_html=True)

# 2. Inject JS with st.html — runs in same page, can find #my-tbl
st.html("""<script>
(function(){
  var tbl = document.getElementById('my-tbl');
  // ... manipulate DOM ...
})();
</script>""", unsafe_allow_javascript=True)
```

### Key gotchas

- `st.html()` does NOT accept a `height` parameter (Streamlit 1.55). Signature: `st.html(body, *, width, unsafe_allow_javascript)`
- For fragment reruns (e.g., `@st.fragment(run_every=5s)`), the DOM is rebuilt each time. Use `localStorage` to persist client-side state (e.g., sort column/direction) across reruns.
- Use a guard like `if(!el._bound) { el._bound=true; ... }` to avoid duplicate event listeners if the script may re-execute.
- Row links that should navigate the full app use `target="_self"` (same as parent page since st.html is not iframe).

### Anti-patterns (DO NOT use)

- `st.markdown()` with `<script>` tags — scripts are silently stripped
- `st.html()` without `unsafe_allow_javascript=True` — scripts are ignored
- Query-param-based sorting (`?sort=col`) — causes full Streamlit rerun, resets active tab, scroll position
- `st.components.v1.html()` for DOM manipulation — creates an iframe, cannot access parent DOM
