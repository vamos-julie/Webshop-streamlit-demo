import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import altair as alt
import os

st.set_page_config(layout="wide")

db_url = os.environ["DATABASE_URL"]
if db_url is None:
    conn = st.connection("postgresql", type="sql")
else:
    conn = st.connection("postgresql", type="sql", url=db_url)

# Define custom CSS for the first style
st.markdown(
    """
    <style>
    .custom-info-1 {
        display: flex;
        align-items: center;
        background-color: #9FE2BF;
        border-left: 4px solid #155724;
        padding: 10px;
        border-radius: 4px;
        color: #155724;
        font-size: 1rem;
        margin-bottom: 20px;
    }
    .custom-info-1 .emoji {
        margin-right: 10px;
        font-size: 1.5rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# Define custom CSS for the second style
st.markdown(
    """
    <style>
    .custom-info-2 {
        display: flex;
        align-items: center;
        background-color: #fefecd;
        border-left: 4px solid #ffd700;
        padding: 10px;
        border-radius: 4px;
        color: #333333;
        font-size: 1rem;
        margin-bottom: 20px;
    }
    .custom-info-2 .emoji {
        margin-right: 10px;
        font-size: 1.5rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.header("Dashboard for Webshop's Managers")

st.markdown(
    """
    <div class="custom-info-2">
        <div class="emoji">📍</div>
        <div>Webshop has strength in diversification between different categories of products and labels. The possible drawbacks are low number of high earning/selling brands and great number of brands with sales less than $4,000.
        This labels might be inefficient costwise, if you consider finding, promoting, maintaining them. Webshop management should conduct further investigation of the most succesful labels on Webshop to identify patterns in promotion and trends in items they sell.
        Apparel and Footwear which are top selling categories but at the same time top selling products belong to Formal Wear and Accessories. This could be an interestting niche.</div>
    </div>
    """,
    unsafe_allow_html=True,
)

st.subheader("Which categories should be our priority?")


st.markdown(
    """
    <div class="custom-info-2">
        <div class="emoji">📍</div>
        <div>In terms of revenues and quantitites of items sold Apparel and Footwear are leading areas for Webshop.</div>
    </div>
    """,
    unsafe_allow_html=True,
)

df_category = conn.query(
    sql="""SELECT p.category, to_char(o.ordertimestamp, 'YYYY-MM') as date_of_sale, COUNT(DATE(o.ordertimestamp)) AS number_of_sales, SUM(o.total) AS revenue 
FROM webshop.order AS o
JOIN webshop.order_positions AS op ON o.id = op.orderid
JOIN webshop.articles AS a ON a.id = op.articleid
JOIN webshop.products AS p ON p.id = a.productid
GROUP BY 1,2
ORDER BY 1,2"""
)
c = (
    (
        alt.Chart(df_category)
        .mark_line()
        .encode(
            x=alt.X("date_of_sale", axis=alt.Axis(title=None)),
            y=alt.Y("number_of_sales", axis=alt.Axis(title=None)),
            color=alt.Color("category", legend=None),
            tooltip=["date_of_sale", "number_of_sales", "category"],
        )
    )
    .configure_axis(
        grid=False, domain=True, ticks=True, labelColor="black", titleColor="black"
    )
    .properties(
        width=200,
        height=400,
        title=alt.TitleParams(
            "Quantities of products sold by category", anchor="middle"
        ),
    )
)
st.altair_chart(c, use_container_width=True)
###############
df_category2 = conn.query(
    sql="""SELECT p.category, to_char(o.ordertimestamp, 'YYYY-MM') as date_of_sale, COUNT(DATE(o.ordertimestamp)) AS number_of_sales, SUM(o.total)::numeric::int AS revenue 
FROM webshop.order AS o
JOIN webshop.order_positions AS op ON o.id = op.orderid
JOIN webshop.articles AS a ON a.id = op.articleid
JOIN webshop.products AS p ON p.id = a.productid
GROUP BY 1,2
ORDER BY 1,2,4"""
)
d = (
    (
        alt.Chart(df_category2)
        .mark_line()
        .encode(
            x=alt.X("date_of_sale", axis=alt.Axis(title=None)),
            y=alt.Y("revenue", axis=alt.Axis(title=None)),
            color=alt.Color(
                "category",
                legend=alt.Legend(title="Categories by color:", orient="bottom"),
            ),
            tooltip=["date_of_sale", "revenue", "category"],
        )
    )
    .configure_axis(
        grid=False, domain=True, ticks=True, labelColor="black", titleColor="black"
    )
    .properties(
        width=200,
        height=500,
        title=alt.TitleParams("Revenue by product category", anchor="middle"),
    )
)
st.altair_chart(d, use_container_width=True)

st.markdown(
    """
    <div class="custom-info-2">
        <div class="emoji">📍</div>
        <div>Sales are equal between male and female products.</div>
    </div>
    """,
    unsafe_allow_html=True,
)

df_gender = conn.query(
    sql="""SELECT p.gender, count(o.id)
FROM webshop.order AS o
JOIN webshop.order_positions AS op ON o.id = op.orderid
JOIN webshop.articles AS a ON a.id = op.articleid
JOIN webshop.products AS p ON p.id = a.productid
GROUP BY 1"""
)
e = (
    alt.Chart(df_gender)
    .mark_arc()
    .encode(
        theta="count",
        color=alt.Color("gender", legend=alt.Legend(title=None, orient="left")),
    )
    .properties(title=alt.TitleParams("Products by gender", anchor="middle"))
)
st.altair_chart(e, use_container_width=True)
###############

st.markdown(
    """
    <div class="custom-info-2">
        <div class="emoji">📍</div>
        <div>There is only slight concentration of revenues and amounts of products sold on specific brands. Top 20 bestselling labels include brands with sales from $9,000.</div>
    </div>
    """,
    unsafe_allow_html=True,
)

df_labels_all = conn.query(
    sql="""WITH labels_revenue AS (SELECT l.name, count(date(o.ordertimestamp)) as number_of_labels, SUM(o.total)::numeric::int AS revenue,
(CASE WHEN SUM(o.total)::numeric::int > 10000 THEN 'More than $10,000'
	  WHEN SUM(o.total)::numeric::int BETWEEN 7000 AND 9999 THEN 'Between $7,000 and $10,000'
	  WHEN SUM(o.total)::numeric::int BETWEEN 4000 AND 6999 THEN 'Between $4,000 and $7,000'
	  ELSE 'Less than $4,000'
END) AS revenue_distribution	
FROM webshop.order AS o
JOIN webshop.order_positions AS op ON o.id = op.orderid
JOIN webshop.articles AS a ON a.id = op.articleid
JOIN webshop.products AS p ON p.id = a.productid
JOIN webshop.labels AS l on l.id = p.labelid
GROUP BY 1)
SELECT revenue_distribution, sum(revenue) AS revenue, sum(number_of_labels) AS number_of_products_sold
FROM labels_revenue
GROUP BY 1
ORDER BY 2"""
)

c01, c02 = st.columns([1, 1])

with c01:
    rev_div_label_pie = (
        alt.Chart(df_labels_all)
        .mark_arc(innerRadius=100)
        .encode(
            theta=alt.Theta(
                field="revenue",
                type="quantitative",
                aggregate="sum",
                title="Total revenue",
            ),
            color=alt.Color(
                "revenue_distribution",
                legend=alt.Legend(title="Total revenue of labels:", orient="left"),
            ),
        )
        .properties(
            width=200,
            height=300,
            title=alt.TitleParams("Labels revenue", anchor="middle"),
        )
        .configure_legend(labelLimit=0)
    )

    st.altair_chart(rev_div_label_pie, use_container_width=True)

with c02:
    number_div_label_pie = (
        alt.Chart(df_labels_all)
        .mark_arc(innerRadius=100)
        .encode(
            theta=alt.Theta(
                field="number_of_products_sold",
                type="quantitative",
                aggregate="sum",
                title="Quantities of products sold for revenue group",
            ),
            color=alt.Color("revenue_distribution", legend=None),
        )
        .properties(
            width=200,
            height=300,
            title=alt.TitleParams("Number of products for label sold", anchor="middle"),
        )
        .configure_legend(labelLimit=0)
    )

    st.altair_chart(number_div_label_pie, use_container_width=True)

###############
df_labels = conn.query(
    sql="""SELECT l.name, count(date(o.ordertimestamp)), SUM(o.total)::numeric::int AS revenue
FROM webshop.order AS o
JOIN webshop.order_positions AS op ON o.id = op.orderid
JOIN webshop.articles AS a ON a.id = op.articleid
JOIN webshop.products AS p ON p.id = a.productid
JOIN webshop.labels AS l on l.id = p.labelid
GROUP BY l.name
ORDER BY 3 DESC LIMIT 20"""
)

g = (
    alt.Chart(df_labels)
    .mark_bar(size=30)
    .encode(
        x=alt.X("name", axis=alt.Axis(title=None)),
        y=alt.Y("revenue", axis=alt.Axis(title=None)),
    )
    .properties(
        width=200,
        height=350,
        title=alt.TitleParams("Top 20 labels by revenue", anchor="middle"),
    )
    .configure_axis(
        grid=False, domain=True, ticks=True, labelColor="black", titleColor="black"
    )
)
st.altair_chart(g, use_container_width=True)


st.markdown(
    """
    <div class="custom-info-2">
        <div class="emoji">📍</div>
        <div>Top selling products for Webshop belong to different categories.
        The product which brought the biggest revenue was a formal wear item Tuxedo Atlan.
        Other popular products were pants and shorts, accessories like belts, wraps and scarves and footwear like boots, flip-flops and shoes.</div>
    </div>
    """,
    unsafe_allow_html=True,
)

df_top_products = conn.query(
    sql="""SELECT name, category, "Total sales volume"
FROM (SELECT p.name, p.category, sum(op.amount*op.price) AS "Total sales volume",
	DENSE_RANK() OVER(ORDER BY sum(op.amount*op.price) DESC) AS top_selling_items
FROM webshop.order AS o
JOIN webshop.order_positions AS op ON o.id = op.orderid
JOIN webshop.articles AS a ON a.id = op.articleid
JOIN webshop.products AS p ON p.id = a.productid
GROUP BY 1,2) as top100
WHERE top_selling_items <= 100"""
)

st.markdown(
    """
    <style>
    .centered-text {
        display: flex;
        justify-content: center;
        align-items: center;
        margin-top: 0px;
        padding: 0px;
        height: 9vh;
        font-size: 1rem; /* Adjust the font size as needed */
        font-weight: bold;
        color: black;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    '<div class="centered-text">Top selling products filtered by category</div>',
    unsafe_allow_html=True,
)

c03, c04 = st.columns([1, 1])
with c03:

    df_categories = conn.query(sql="""SELECT DISTINCT category FROM webshop.products""")
    df_categories["Choose category"] = [
        True for i in range(len(df_categories["category"]))
    ]

    edited_df_categories = st.data_editor(
        df_categories,
        column_config={
            "Choose category": st.column_config.CheckboxColumn(
                "Which category?",
                help="Select category/ies",
                default=True,
            )
        },
        disabled=["category"],
        hide_index=True,
    )
with c04:
    selected_categories = edited_df_categories[
        edited_df_categories["Choose category"] == True
    ]["category"].to_list()

    if len(selected_categories) == 0:
        st.write("No category is selected")
    else:
        df_top_products.index = np.arange(1, len(df_top_products.index) + 1)
        st.write(df_top_products[df_top_products["category"].isin(selected_categories)])

st.markdown(
    """
    <div class="custom-info-1">
        <div class="emoji">💡</div>
        <div>Webshop has strength in diversification between different categories of products and labels. The possible drawbacks are low number of high earning/selling brands and great number of brands with sales less than $4,000.
        This labels might be inefficient costwise, if you consider finding, promoting, maintaining them. Webshop management should conduct further investigation of the most succesful labels on Webshop to identify patterns in promotion and trends in items they sell.
        Apparel and Footwear which are top selling categories but at the same time top selling products belong to Formal Wear and Accessories. This could be an interestting niche.</div>
    </div>
    """,
    unsafe_allow_html=True,
)

st.subheader("Is our pricing strategy working?")

st.markdown(
    """
    <div class="custom-info-2">
        <div class="emoji">📍</div>
        <div>Only 18 products out of 917 benefited from price reduction, in other words they were sold more after their price was decreased.
        322 products were sold solely on discount, 295 products were sold without any discounts, 282 products were not sold so far at all.</div>
    </div>
    """,
    unsafe_allow_html=True,
)

sql = """WITH product_prices AS 
	(SELECT p.name, to_char(o.ordertimestamp, 'YYYY-MM-DD') as order_date, op.price AS sales_price, a.originalprice AS original_price, a.reducedprice AS reduced_price, o.id, p.category, p.gender
	FROM webshop.order AS o
	JOIN webshop.order_positions AS op ON o.id = op.orderid
	JOIN webshop.articles AS a ON a.id = op.articleid
	JOIN webshop.products AS p ON p.id = a.productid
	ORDER BY 1,2), discount_or_not AS
	(SELECT name, order_date, category, gender,
	(CASE WHEN (original_price - sales_price) ::numeric::int > 0
		  THEN 1
		  ELSE 0
	 END) AS disc_sale
	FROM product_prices)
SELECT category, count(order_date), sum(disc_sale), round(cast(sum(disc_sale) as decimal)/count(order_date)*100,2) as discounted_sales_percentage
FROM discount_or_not
GROUP BY category
ORDER BY 4 DESC"""
df_pricing_categories = conn.query(sql=sql)

h = (
    alt.Chart(df_pricing_categories)
    .encode(
        theta=alt.Theta("discounted_sales_percentage:Q", stack=True),
        radius=alt.Radius(
            "discounted_sales_percentage",
            scale=alt.Scale(type="sqrt", zero=True, rangeMin=20),
        ),
        color=alt.Color(
            "category:N", legend=alt.Legend(title="Category by color:", orient="right")
        ),
    )
    .properties(
        width=200,
        height=350,
        title=alt.TitleParams("Sales of discounted products", anchor="middle"),
    )
)

h1 = h.mark_arc(innerRadius=20, stroke="#fff")
h2 = h.mark_text(radiusOffset=10).encode(text="discounted_sales_percentage:Q")
chart = h1 + h2


c1, c2 = st.columns([1, 1])
with c1:
    st.altair_chart(chart, theme="streamlit", use_container_width=True)
with c2:
    st.expander("See code").code(sql)

st.markdown(
    """
    <div class="custom-info-2">
        <div class="emoji">📍</div>
        <div>Cosmetics, Luggage and Footwear were sold on discount the most.</div>
    </div>
    """,
    unsafe_allow_html=True,
)

with st.container(border=True):

    st.markdown(
        """
        <div class="custom-info-2">
            <div class="emoji">📍</div>
            <div>Out of 1616 orders with <strong>more than one</strong> product 1448 orders contained at least one product with discount.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    code = """WITH order_compositions AS 
        (SELECT o.id AS order_id, op.id AS order_positions_id, p.name, p.category, p.gender, op.price AS sales_price, a.originalprice AS original_price, a.reducedprice AS reduced_price 
        FROM webshop.order AS o
        JOIN webshop.order_positions AS op ON o.id = op.orderid
        JOIN webshop.articles AS a ON a.id = op.articleid
        JOIN webshop.products AS p ON p.id = a.productid
        ORDER BY 1,2), discount_or_not AS
        (SELECT name, order_id, order_positions_id, category, gender,
        (CASE WHEN (original_price - sales_price) ::numeric::int > 0
            THEN 1
            ELSE 0
        END) AS disc_sale
        FROM order_compositions)
    SELECT order_id, count(order_positions_id) AS number_of_items, sum(disc_sale), round(cast(sum(disc_sale) as decimal)/count(order_positions_id)*100,2) as discounted_sales_percentage
    FROM discount_or_not
    GROUP BY order_id
    HAVING count(order_positions_id) > 1
    ORDER BY 4 DESC
    """
    st.expander("See code").code(code)


with st.container(border=True):
    st.markdown(
        """
        <div class="custom-info-2">
            <div class="emoji">📍</div>
            <div>Out of 1616 orders with <strong>more than one</strong> product in 1072 orders were at least one item on sale from such categories as Cosmetics, Luggage and Footwear.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    code2 = """WITH order_compositions AS 
        (SELECT o.id AS order_id, op.id AS order_positions_id, p.name, p.category, p.gender, op.price AS sales_price, a.originalprice AS original_price, a.reducedprice AS reduced_price 
        FROM webshop.order AS o
        JOIN webshop.order_positions AS op ON o.id = op.orderid
        JOIN webshop.articles AS a ON a.id = op.articleid
        JOIN webshop.products AS p ON p.id = a.productid
        ORDER BY 1,2), discount_or_not AS
        (SELECT name, order_id, order_positions_id, category, gender,
        (CASE WHEN (original_price - sales_price) ::numeric::int > 0
            THEN 1
            ELSE 0
        END) AS disc_sale
        FROM order_compositions), category_division AS 
        (SELECT name, order_id, order_positions_id, category, gender, disc_sale,
        (CASE WHEN category = 'Apparel' THEN 1
            ELSE 0
        END) AS apparel,
        (CASE WHEN category = 'Footwear' THEN 1
            ELSE 0
        END) AS footwear,
        (CASE WHEN category = 'Luggage' THEN 1
            ELSE 0
        END) AS luggage,
        (CASE WHEN category = 'Sportswear' THEN 1
            ELSE 0
        END) AS sportswear,
        (CASE WHEN category = 'Traditional' THEN 1
            ELSE 0
        END) AS traditional,
        (CASE WHEN category = 'Formal Wear' THEN 1
            ELSE 0
        END) AS formal_wear,
        (CASE WHEN category = 'Cosmetics' THEN 1
            ELSE 0
        END) AS cosmetics,
        (CASE WHEN category = 'Accessories' THEN 1
            ELSE 0
        END) AS accessories,
        (CASE WHEN category = 'Watches & Jewelry' THEN 1
            ELSE 0
        END) AS watches
        FROM discount_or_not)
    SELECT order_id, count(order_positions_id) AS number_of_items, sum(disc_sale), round(cast(sum(disc_sale) as decimal)/count(order_positions_id)*100,2) as discounted_sales_percentage, sum(apparel) as sapparel,sum(footwear) as sfootwear, sum(accessories) as saccessories, sum(cosmetics) as scosmetics, sum(formal_wear) as sformal_wear, sum(luggage) as sluggage, sum(sportswear) as ssportswear, sum(traditional) as straditional, sum(watches) as swatches  
    FROM category_division
    GROUP BY order_id
    HAVING count(order_positions_id) > 1 AND (sum(cosmetics) > 0 OR sum(luggage) > 0 OR sum(footwear) > 0) AND sum(disc_sale) > 0
    ORDER BY 4 ASC
        """
    st.expander("See code").code(code2)


st.markdown(
    """
    <div class="custom-info-1">
        <div class="emoji">💡</div>
        <div>Something seems odd with pricing strategy, because only 18 products were sold after they were discounted.
        Management should investigate the reasons further: maybe Webshop doesn't efficiently convey price reductions to customers or the timing for price reductions is wrong.
        Research has shown that more than a half of orders contain discounted items. Customers prefer to buy Cosmetics, Luggage and Footwear on sale.
        This behavior should be investigated further. As customers might create their shopping basket around Apparel, Traditional or Footwear item and impulsively add passing accessories and shoes, if they have a discount. In this case it might be beneficial to offer personal discounts on accessories and shoes, if specific apparel clothing is in the shopping basket.</div>
    </div>
    """,
    unsafe_allow_html=True,
)

st.subheader("Who are our customers?")

st.markdown(
    """
    <div class="custom-info-2">
        <div class="emoji">📍</div>
        <div>Webshop has had 868 customers so far.</div>
    </div>
    """,
    unsafe_allow_html=True,
)

df_customers_gender = conn.query(
    sql="""WITH customer_data AS (SELECT c.id as customer_id, c.gender, EXTRACT(year FROM age(current_date,c.dateofbirth)) :: int AS Age, a.city, count(Distinct o.id) AS Number_of_orders, count(op.id) AS Number_of_products_bought, sum(o.total) AS Money_spent, sum(o.total)/count(op.id) AS av  
	FROM webshop.customer AS c LEFT JOIN webshop.address AS a ON c.id = a.customerid
	JOIN webshop.order AS o ON o.customer = c.id
	JOIN webshop.order_positions AS op ON op.orderid = o.id
	GROUP BY 1,4
	ORDER BY 7 DESC, 6 DESC)
SELECT gender, count(customer_id) AS number_of_customers_per_gender
FROM customer_data
GROUP BY 1
ORDER BY 2 DESC"""
)

df_customers_age = conn.query(
    sql="""WITH customer_data AS (SELECT c.id as customer_id, c.gender, EXTRACT(year FROM age(current_date,c.dateofbirth)) :: int AS Age, a.city, count(Distinct o.id) AS Number_of_orders, count(op.id) AS Number_of_products_bought, sum(o.total) AS Money_spent, sum(o.total)/count(op.id) AS av  
	FROM webshop.customer AS c LEFT JOIN webshop.address AS a ON c.id = a.customerid
	JOIN webshop.order AS o ON o.customer = c.id
	JOIN webshop.order_positions AS op ON op.orderid = o.id
	GROUP BY 1,4
	ORDER BY 7 DESC, 6 DESC), age_structure AS (SELECT customer_id, age,
		(CASE WHEN age BETWEEN 18 AND 30 THEN '18-30'
		WHEN age BETWEEN 31 AND 40 THEN '31-40'
		WHEN age BETWEEN 41 AND 50 THEN '41-50'
		WHEN age BETWEEN 51 AND 65 THEN '51-65'
		WHEN age>65 THEN '66+'
		ELSE '<18'
		END) as age_group
	FROM customer_data)
SELECT age_group, count(age_group) AS "Age group"
FROM age_structure
GROUP BY 1
ORDER BY 1 """
)

c05, c06 = st.columns([1, 1])

with c05:
    customer_gender_pie = (
        alt.Chart(df_customers_gender)
        .mark_arc(innerRadius=100)
        .encode(
            theta=alt.Theta(
                field="number_of_customers_per_gender",
                type="quantitative",
                aggregate="sum",
                title="Distribution of customers by gender",
            ),
            color=alt.Color(
                "gender", legend=alt.Legend(title="Customers by gender:", orient="left")
            ),
        )
        .properties(
            width=200,
            height=300,
            title=alt.TitleParams(
                "Distribution of customers by gender", anchor="middle"
            ),
        )
        .configure_legend(labelLimit=0)
    )

    st.altair_chart(customer_gender_pie, use_container_width=True)

with c06:
    customer_age_pie = (
        alt.Chart(df_customers_age)
        .mark_arc(innerRadius=100)
        .encode(
            theta=alt.Theta(
                field="Age group",
                type="quantitative",
                aggregate="sum",
                title="Distribution of customers by age",
            ),
            color=alt.Color(
                "age_group",
                legend=alt.Legend(title="Customers by age:", orient="right"),
            ),
        )
        .properties(
            width=200,
            height=300,
            title=alt.TitleParams("Distribution of customers by age", anchor="middle"),
        )
        .configure_legend(labelLimit=0)
    )

    st.altair_chart(customer_age_pie, use_container_width=True)

df_age_group_summary = conn.query(
    sql="""WITH customer_data AS (SELECT c.id as customer_id, c.gender, EXTRACT(year FROM age(current_date,c.dateofbirth)) :: int AS Age, a.city, count(Distinct o.id) AS Number_of_orders, count(op.id) AS Number_of_products_bought, sum(o.total)::numeric::int AS Money_spent, (sum(o.total)/count(o.id))::numeric::int AS average_check  
	FROM webshop.customer AS c LEFT JOIN webshop.address AS a ON c.id = a.customerid
	JOIN webshop.order AS o ON o.customer = c.id
	JOIN webshop.order_positions AS op ON op.orderid = o.id
	GROUP BY 1,4
	ORDER BY 7 DESC, 6 DESC), age_structure AS (SELECT *,
		(CASE WHEN age BETWEEN 18 AND 30 THEN '18-30'
		WHEN age BETWEEN 31 AND 40 THEN '31-40'
		WHEN age BETWEEN 41 AND 50 THEN '41-50'
		WHEN age BETWEEN 51 AND 65 THEN '51-65'
		WHEN age>65 THEN '66+'
		ELSE '<18'
		END) as age_group
	FROM customer_data)
SELECT age_group, count(age_group) AS number_of_customers_per_age_group, ROUND(AVG(Number_of_orders),2) AS average_number_of_orders_per_age_group, ROUND(AVG(Number_of_products_bought),2) AS average_products_bought_per_age_group, ROUND(AVG(money_spent),2) AS average_money_spent_per_age_group, ROUND(AVG(average_check),2) AS average_check_per_age_group
FROM age_structure
GROUP BY 1
ORDER BY 1
"""
)

st.dataframe(data=df_age_group_summary, hide_index=True)

code3 = """WITH customer_data AS (SELECT c.id as customer_id, c.gender, EXTRACT(year FROM age(current_date,c.dateofbirth)) :: int AS Age, a.city, count(Distinct o.id) AS Number_of_orders, count(op.id) AS Number_of_products_bought, sum(o.total)::numeric::int AS Money_spent, (sum(o.total)/count(op.id))::numeric::int AS average_check  
	FROM webshop.customer AS c LEFT JOIN webshop.address AS a ON c.id = a.customerid
	JOIN webshop.order AS o ON o.customer = c.id
	JOIN webshop.order_positions AS op ON op.orderid = o.id
	GROUP BY 1,4
	ORDER BY 7 DESC, 6 DESC), age_structure AS (SELECT *,
		(CASE WHEN age BETWEEN 18 AND 30 THEN '18-30'
		WHEN age BETWEEN 31 AND 40 THEN '31-40'
		WHEN age BETWEEN 41 AND 50 THEN '41-50'
		WHEN age BETWEEN 51 AND 65 THEN '51-65'
		WHEN age>65 THEN '66+'
		ELSE '<18'
		END) as age_group
	FROM customer_data)
SELECT age_group, count(age_group) AS number_of_customers_per_age_group, ROUND(AVG(Number_of_orders),2) AS average_number_of_orders_per_age_group, ROUND(AVG(Number_of_products_bought),2) AS average_products_bought_per_age_group, ROUND(AVG(money_spent),2) AS average_money_spent_per_age_group, ROUND(AVG(average_check),2) AS average_check_per_age_group
FROM age_structure
GROUP BY 1
ORDER BY 1
"""
st.expander("See code").code(code3)
st.markdown(
    """
    <div class="custom-info-2">
        <div class="emoji">📍</div>
        <div>Webshop has even distribution between male and female customers.
        Interesting finding is that more than a half of customers are older than 50, and three quarters of customers are older than 40.
        Customers over 65 spend the most on purchases.</div>
    </div>
    """,
    unsafe_allow_html=True,
)

df_customers = conn.query(
    sql="""SELECT c.id as customer_id, c.gender, EXTRACT(year FROM age(current_date,c.dateofbirth)) :: int AS Age, a.city, count(Distinct o.id) AS Number_of_orders, count(op.id) AS Number_of_products_bought, sum(o.total)::numeric::int AS Money_spent, (sum(o.total)/count(o.id))::numeric::int AS average_check  
	FROM webshop.customer AS c LEFT JOIN webshop.address AS a ON c.id = a.customerid
	JOIN webshop.order AS o ON o.customer = c.id
	JOIN webshop.order_positions AS op ON op.orderid = o.id
	GROUP BY 1,4
	ORDER BY 7 DESC, 6 DESC"""
)

custom_colors = ["#1f77b4", "#ff7f0e"]
customers_revenue_scatter = (
    alt.Chart(df_customers)
    .mark_circle()
    .encode(
        x=alt.X("money_spent", axis=alt.Axis(title="Money spent in $")),
        y=alt.Y("average_check", axis=alt.Axis(title="Average check per order in $")),
        color=alt.Color(
            "gender",
            legend=alt.Legend(title="Gender:", orient="right"),
            scale=alt.Scale(domain=["male", "female"], range=custom_colors),
        ),
    )
    .configure_axis(
        grid=False, domain=True, ticks=True, labelColor="black", titleColor="black"
    )
    .interactive()
)

st.altair_chart(customers_revenue_scatter, theme="streamlit", use_container_width=True)


st.markdown(
    """
    <div class="custom-info-2">
        <div class="emoji">📍</div>
        <div>Most of high-spenders on Webshop are women, but on average amounts of money spent by representatives of each gender is equal.</div>
    </div>
    """,
    unsafe_allow_html=True,
)

df_age_group_summary_short = conn.query(
    sql="""WITH customer_analysis AS 
	(SELECT c.id as customer_id, c.gender, c.dateofbirth, EXTRACT(year FROM age(current_date,c.dateofbirth)) :: int AS age, a.city, count(o.id) AS number_of_purchases, sum(o.total) AS money_spent, AVG(COALESCE(ar.discountinpercent,0)) as discounts 
	FROM webshop.customer AS c LEFT JOIN webshop.address AS a ON c.id = a.customerid
	JOIN webshop.order AS o ON o.customer = c.id
	JOIN webshop.order_positions AS op ON op.orderid = o.id
	JOIN webshop.articles AS ar ON op.articleid = ar.id
	GROUP BY 1,5
	HAVING count(o.id) > 1
	ORDER BY 7 DESC, 6 DESC), age_structure AS (SELECT *,
		(CASE WHEN age BETWEEN 18 AND 30 THEN '18-30'
		WHEN age BETWEEN 31 AND 40 THEN '31-40'
		WHEN age BETWEEN 41 AND 50 THEN '41-50'
		WHEN age BETWEEN 51 AND 65 THEN '51-65'
		WHEN age>65 THEN '66+'
		ELSE '<18'
		END) as age_group
	FROM customer_analysis)
SELECT age_group, round(avg((cast(money_spent as decimal)/number_of_purchases)),2) AS average_check, round(AVG(discounts),2) AS average_discount
FROM age_structure
GROUP BY age_group
ORDER BY 1 ASC"""
)


base = alt.Chart(df_age_group_summary_short).encode(
    x=alt.X("age_group", axis=alt.Axis(title="Age group"))
)
bar1 = base.mark_bar(color="#19AAD0").encode(
    y=alt.Y("average_check", axis=alt.Axis(title="Average check per order in $"))
)

# Text labels
text1 = base.mark_text(
    align="center",
    baseline="top",
    dy=-80,  # Adjust this value to move the text position as needed
    color="black",
).encode(text="average_check:Q")

# Combine bar chart and text labels
chart1 = bar1 + text1


line = base.mark_line(color="#ff7f0e", size=5).encode(
    y=alt.Y(
        "average_discount:Q",
        axis=alt.Axis(title="Average Discount"),
        scale=alt.Scale(domain=[11, 14]),
    ),
    color=alt.value("#ff7f0e"),
)
chart2 = (
    alt.layer(chart1, line)
    .resolve_scale(y="independent")  # This specifies that the y-axes are independent
    .properties(
        width=600,
        title=alt.TitleParams(
            "Average Check and Discount by Age Group of Recurring Customers",
            anchor="middle",
        ),
    )
)
st.altair_chart(chart2, theme="streamlit", use_container_width=True)


st.markdown(
    """
    <div class="custom-info-2">
        <div class="emoji">📍</div>
        <div>Recurring customers have slightly higher average checks than customers, who have made only one purchase.
        Also recurring customers use discounts, but their average value is around 12-13%.</div>
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="custom-info-1">
        <div class="emoji">💡</div>
        <div>From the data above we see that Webshop is popular with people above 50. THey become recurring clients and spend more money on Webshop.
        Management should investigate advertising methods, promotion techniques and website organisation, which led to success among more senior clients.
        As recurring clients buy with average discount of 12-13% we can offer 10-15 percent discount on second orders.</div>
    </div>
    """,
    unsafe_allow_html=True,
)

st.subheader(
    "Do we have enough stock to achieve growth? What items popular items do we have to restock?"
)

st.markdown(
    """
    <div class="custom-info-2">
        <div class="emoji">📍</div>
        <div>The table below shows all products which were popular in the past time periods and are now sold out and almost sold out.</div>
    </div>
    """,
    unsafe_allow_html=True,
)


df_stock = conn.query(
    sql="""WITH low_stock AS 
	(Select st.articleid as stock_article_id, st.count as quantity_left, col.name as color, si.size, p.name, p.category
	From webshop.stock as st
	JOIN webshop.articles as ar ON st.articleid = ar.id
	JOIN webshop.products as p ON p.id = ar.productid
	JOIN webshop.colors as col ON ar.colorid = col.id
	JOIN webshop.sizes as si ON si.id = ar.size
	WHERE st.count < 2), popular_articles AS (
	SELECT op.articleid order_article_id,sum(amount)
	FROM webshop.order_positions as op
	GROUP BY 1
	HAVING sum(amount) > 1)
SELECT stock_article_id :: text,name, color, size, category, quantity_left
FROM low_stock
JOIN popular_articles ON stock_article_id = order_article_id"""
)


c08, c09, c10 = st.columns([1, 1.25, 2.75])


with c08:

    df_sizes_2 = conn.query(sql="""SELECT DISTINCT size FROM webshop.sizes""")
    df_sizes_2["Choose size"] = [True for i in range(len(df_sizes_2["size"]))]

    edited_df_size_2 = st.data_editor(
        df_sizes_2,
        column_config={
            "Select size": st.column_config.CheckboxColumn(
                "Which size?",
                help="Select size",
                default=False,
            )
        },
        disabled=["size"],
        hide_index=True,
    )
    selected_sizes_2 = edited_df_size_2[edited_df_size_2["Choose size"] == True][
        "size"
    ].to_list()

with c09:

    df_categories_2 = conn.query(
        sql="""SELECT DISTINCT category FROM webshop.products"""
    )
    df_categories_2["Choose category"] = [
        True for i in range(len(df_categories_2["category"]))
    ]

    edited_df_categories_2 = st.data_editor(
        df_categories_2,
        column_config={
            "Select category": st.column_config.CheckboxColumn(
                "Which category?",
                help="Select category/ies",
                default=True,
            )
        },
        disabled=["category"],
        hide_index=True,
    )
with c10:
    selected_categories_2 = edited_df_categories_2[
        edited_df_categories_2["Choose category"] == True
    ]["category"].to_list()

    if len(selected_categories_2) == 0:
        st.write("No category is selected")
    elif len(selected_sizes_2) == 0:
        st.write("No size is selected")
    else:
        df_stock.index = np.arange(1, len(df_stock.index) + 1)
        st.write(
            df_stock[
                df_stock["category"].isin(selected_categories_2)
                & df_stock["size"].isin(selected_sizes_2)
            ]
        )


st.markdown(
    """
    <div class="custom-info-1">
        <div class="emoji">💡</div>
        <div>This tool shows that many popular items need restocking as they were bought out by the customers, e.g. the highest selling item Tuxedo Atlan was sold out.
        Webshop should make a tool for assesing popular items to achieve better restocking opportunities for them.</div>
    </div>
    """,
    unsafe_allow_html=True,
)
