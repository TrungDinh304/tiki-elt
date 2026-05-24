"""System prompt and context formatter for the product consultant chatbot."""
from __future__ import annotations

from rag import Hit

SYSTEM_PROMPT = """Bạn là trợ lý tư vấn mua sắm cho Tiki.vn. Bạn chỉ tư vấn dựa trên
danh sách sản phẩm được cung cấp trong phần "Sản phẩm liên quan" bên dưới —
KHÔNG bịa thêm sản phẩm không có trong danh sách.

Quy tắc trả lời:
1. Trả lời ngắn gọn, tiếng Việt, thân thiện.
2. Khi đề xuất sản phẩm, luôn nêu tên + giá + rating, và trích dẫn theo
   định dạng [#1], [#2]... khớp với số thứ tự trong danh sách.
3. Nếu danh sách rỗng hoặc không có sản phẩm phù hợp, nói rõ chưa tìm thấy
   và đề nghị người dùng mô tả rõ hơn (giá, thương hiệu, mục đích sử dụng).
4. So sánh trung lập: nêu cả ưu và nhược điểm dựa trên rating và review.
"""


def format_context(hits: list[Hit]) -> str:
    if not hits:
        return "Sản phẩm liên quan: (không tìm thấy)"
    lines = ["Sản phẩm liên quan:"]
    for i, h in enumerate(hits, 1):
        price_str = f"{h.price:,.0f}đ" if h.price is not None else "?"
        rating_str = (
            f"{h.rating_average:.1f}★ ({h.review_count or 0} reviews)"
            if h.rating_average is not None
            else "chưa có rating"
        )
        lines.append(
            f"\n[#{i}] {h.product_name}\n"
            f"  Thương hiệu: {h.brand_name or '?'} | Danh mục: {h.category_name or '?'}\n"
            f"  Giá: {price_str} | Đánh giá: {rating_str}\n"
            f"  Seller: {h.seller_name or '?'}\n"
            f"  Chi tiết: {h.document[:600]}"
        )
    return "\n".join(lines)
