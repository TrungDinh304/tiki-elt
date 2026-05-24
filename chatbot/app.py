"""Streamlit chat UI for the Tiki product consultant.

Flow per user turn:
  user query → embed → pgvector retrieval (with optional structured filters)
  → build context → stream chat completion from ds2api → display.

Sidebar filters (price range, min rating, category keyword) are applied as
WHERE clauses before the vector search so high-selectivity filters keep
recall high.
"""
from __future__ import annotations

import streamlit as st

from embeddings import embed
from llm import chat_stream
from prompts import SYSTEM_PROMPT, format_context
from rag import RetrievalFilter, retrieve

st.set_page_config(page_title="Tiki Tư Vấn Sản Phẩm", page_icon=":books:", layout="wide")
st.title("Tiki — Trợ lý tư vấn sản phẩm")
st.caption("RAG trên data mart Tiki | pgvector + DeepSeek (qua ds2api)")

with st.sidebar:
    st.header("Bộ lọc")
    price_range = st.slider(
        "Khoảng giá (đ)",
        min_value=0,
        max_value=2_000_000,
        value=(0, 2_000_000),
        step=10_000,
    )
    min_rating = st.slider("Rating tối thiểu", 0.0, 5.0, 0.0, 0.5)
    category_kw = st.text_input("Từ khoá danh mục (ILIKE)", value="")
    top_k = st.number_input("Top-K retrieval", 1, 20, 5)
    if st.button("Xoá lịch sử"):
        st.session_state.history = []
        st.rerun()

if "history" not in st.session_state:
    st.session_state.history = []

for msg in st.session_state.history:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

user_msg = st.chat_input("Bạn cần tư vấn gì? (vd: 'Sách self-help giá dưới 200k rating tốt')")
if user_msg:
    with st.chat_message("user"):
        st.markdown(user_msg)

    with st.chat_message("assistant"):
        with st.spinner("Đang tìm sản phẩm phù hợp..."):
            q_vec = embed(user_msg)
            filt = RetrievalFilter(
                min_price=float(price_range[0]) if price_range[0] > 0 else None,
                max_price=float(price_range[1]) if price_range[1] < 2_000_000 else None,
                min_rating=min_rating if min_rating > 0 else None,
                category_keyword=category_kw.strip() or None,
            )
            hits = retrieve(q_vec, filt, k=int(top_k))

        with st.expander(f"Sản phẩm retrieval ({len(hits)} hit)", expanded=False):
            for i, h in enumerate(hits, 1):
                st.markdown(
                    f"**[#{i}] {h.product_name}** — "
                    f"{h.price:,.0f}đ" if h.price else f"**[#{i}] {h.product_name}**"
                )
                st.caption(
                    f"sim={h.similarity:.3f} | rating={h.rating_average} "
                    f"| reviews={h.review_count} | seller={h.seller_name}"
                )

        context = format_context(hits)
        # Prepend retrieved context as a user-role message so the chat history
        # stays clean — only the natural-language turns end up in history.
        augmented_user = f"{context}\n\nCâu hỏi của khách: {user_msg}"
        placeholder = st.empty()
        acc = ""
        for chunk in chat_stream(SYSTEM_PROMPT, st.session_state.history, augmented_user):
            acc += chunk
            placeholder.markdown(acc + "▌")
        placeholder.markdown(acc)

    st.session_state.history.append({"role": "user", "content": user_msg})
    st.session_state.history.append({"role": "assistant", "content": acc})
