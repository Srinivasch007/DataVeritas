"""
Recon page - Display source/target comparison results
"""
import streamlit as st


def render():
    """Render the Recon page main content."""
    if "recon_excel_df" in st.session_state:
        st.subheader("Uploaded recon file")
        st.dataframe(st.session_state["recon_excel_df"], use_container_width=True, hide_index=True)
        st.markdown("---")

    if "recon_results" not in st.session_state:
        return

    results = st.session_state["recon_results"]
    for r in results:
        sno = r.get("sno", "?")
        label = f"Row {sno}"
        if "error" in r:
            label += f" — Error"
        else:
            label += f": {r.get('source_table', '')} → {r.get('target_table', '')}"
        with st.expander(label, expanded=(len(results) == 1)):
            if "error" in r:
                st.error(r["error"])
            else:
                not_in_target = r.get("columns_not_in_target", [])
                st.markdown("**Columns not found in target table**")
                if not_in_target:
                    st.markdown(", ".join(not_in_target))
                else:
                    st.markdown("*All source columns found in target.*")
                st.markdown("---")
                matching = r.get("matching_columns", [])
                st.markdown("**Matching columns**")
                st.markdown(", ".join(matching) if matching else "*No matching columns.*")
                mismatch_df = r.get("mismatch_df")
                if mismatch_df is not None and not mismatch_df.empty:
                    st.markdown("---")
                    st.markdown("**Mismatched rows (top 10)**")
                    st.dataframe(mismatch_df, use_container_width=True, hide_index=True)
                else:
                    st.markdown("---")
                    st.markdown("*No mismatches detected (top 10 view).*")

                src_df = r.get("source_df")
                if src_df is not None and not src_df.empty:
                    st.markdown("---")
                    st.markdown("**Source table data (matching columns, top 10)**")
                    st.dataframe(src_df, use_container_width=True, hide_index=True)
                tgt_df = r.get("target_df")
                if tgt_df is not None and not tgt_df.empty:
                    st.markdown("---")
                    st.markdown("**Target table data (matching columns, top 10)**")
                    st.dataframe(tgt_df, use_container_width=True, hide_index=True)
