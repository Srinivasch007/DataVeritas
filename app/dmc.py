"""
DMC page - Data Copy Validation
"""
import streamlit as st


def render():
    """Render the DMC page content."""
    if "dmc_excel_df" in st.session_state:
        st.subheader("Data Copy Validation")
        st.dataframe(st.session_state["dmc_excel_df"], use_container_width=True, hide_index=True)

    if "dmc_final_df" in st.session_state:
        final_df = st.session_state["dmc_final_df"]
        if not final_df.empty and "Grouping" in final_df.columns:
            st.markdown("---")
            st.subheader("Final Results (by Grouping)")
            display_cols = [c for c in ["tablename", "count"] if c in final_df.columns]
            groupings = final_df["Grouping"].unique().tolist()
            cols = st.columns(len(groupings) if groupings else 1)
            for i, grouping in enumerate(groupings):
                with cols[i]:
                    st.markdown(f"**{grouping}**")
                    grp = final_df[final_df["Grouping"] == grouping]
                    out = grp[display_cols].copy()
                    out.columns = ["Table Name", "Count"][:len(display_cols)]
                    st.dataframe(out, use_container_width=True, hide_index=True)
        else:
            _render_hop_results()
    elif "dmc_results" in st.session_state:
        _render_hop_results()


def _render_hop_results():
    """Display results by Hop Name, side by side."""
    st.markdown("---")
    st.subheader("Query Results (by Hop Name)")
    results = st.session_state["dmc_results"]
    hop_names = list(results.keys())
    if not hop_names:
        return
    cols = st.columns(len(hop_names))
    for i, hop_name in enumerate(hop_names):
        with cols[i]:
            r = results[hop_name]
            st.markdown(f"**{hop_name}**")
            st.markdown("*SQL*")
            st.code(r.get("sql", ""), language="sql")
            if r.get("error"):
                st.error(r["error"])
            elif r.get("df") is not None:
                st.markdown("*Results*")
                st.dataframe(r["df"], use_container_width=True, hide_index=True)
            else:
                st.info("No data returned.")
