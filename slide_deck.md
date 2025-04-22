**Slide 1: Research Question (Issue)**

**Which type of textual information in IPO prospectuses best predicts future performance and delisting risk?**

- Compare three approaches:
  1. Human-defined dictionary (Loughran-McDonald)
  2. Cosine similarity (informativeness)
  3. LLM-based sentiment (e.g., FinBERT)

**Secondary Issue:**
- Do IPO firms adjust language over time to influence machine-based interpretation?

---

**Slide 2: Why Does This Matter? (Impact)**

- Investors increasingly rely on automated textual signals
- SEC encourages disclosure transparency — but is it gameable?
- Underwriters may use text models in due diligence and pricing
- If firms can "trick" the machines, this threatens the value of textual analysis

---

**Slide 3: Textual Measures Overview**

- **Loughran-McDonald Dictionary**: Count of positive, negative, uncertain words
- **Cosine Similarity**: Distance from average prospectus — "informativeness"
- **FinBERT Sentiment Score**: LLM-based positive/negative/neutral confidence

Example output for a real IPO:
- LM score: -2.4 (net tone)
- Cosine: 0.89 (high similarity = low uniqueness)
- FinBERT: Positive 64%, Neutral 30%, Negative 6%

---

**Slide 4: Initial Patterns from the Data**

- Over time, **FinBERT positive sentiment increases** on average, while cosine similarity stays flat
- **Predictive power of LM dictionary declines** in recent years
- Hypothesis: Firms are learning to optimize impression to machines

(Figure: time trend of FinBERT score vs. IPO underpricing or delisting)

---

**Slide 5: Hypotheses and Approach**

1. **H1:** FinBERT-based measures outperform others in predicting short-run return and long-run delisting
2. **H2:** Predictive power of textual measures declines over time
3. **H3:** Firms with weaker fundamentals have more positive FinBERT tone → strategic manipulation

Empirical Strategy:
- Regress return and delisting on text measures + controls (firm size, age, underwriter)
- Time interaction terms to detect evolution
- Survival models for delisting (Cox)

---

**Slide 6: Implications and Risks**

- If FinBERT wins: Encourage machine-readable disclosure but beware of gaming
- If predictiveness fades: Evidence of adaptation, time to revise investor tools
- Raises concern: Can we detect or audit "textual manipulation"?
- Suggest need for robust, interpretable, and manipulation-resistant models

---

(Optional appendix: Example text from a manipulated vs. honest prospectus)

