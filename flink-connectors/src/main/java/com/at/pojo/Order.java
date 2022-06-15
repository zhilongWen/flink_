package com.at.pojo;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Objects;

/**
 * @create 2022-06-08
 */
public class Order {

    private int order_isd;
    private Date order_date;
    private String customer_name;
    private BigDecimal price;
    private int product_id;
    private boolean order_status;


    public Order() {
    }

    public Order(int order_isd, Date order_date, String customer_name, BigDecimal price, int product_id, boolean order_status) {
        this.order_isd = order_isd;
        this.order_date = order_date;
        this.customer_name = customer_name;
        this.price = price;
        this.product_id = product_id;
        this.order_status = order_status;
    }

    public int getOrder_isd() {
        return order_isd;
    }

    public void setOrder_isd(int order_isd) {
        this.order_isd = order_isd;
    }

    public Date getOrder_date() {
        return order_date;
    }

    public void setOrder_date(Date order_date) {
        this.order_date = order_date;
    }

    public String getCustomer_name() {
        return customer_name;
    }

    public void setCustomer_name(String customer_name) {
        this.customer_name = customer_name;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }

    public int getProduct_id() {
        return product_id;
    }

    public void setProduct_id(int product_id) {
        this.product_id = product_id;
    }

    public boolean isOrder_status() {
        return order_status;
    }

    public void setOrder_status(boolean order_status) {
        this.order_status = order_status;
    }

    @Override
    public String toString() {
        return "Order{" +
                "order_isd=" + order_isd +
                ", order_date=" + order_date +
                ", customer_name='" + customer_name + '\'' +
                ", price=" + price +
                ", product_id=" + product_id +
                ", order_status=" + order_status +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Order order = (Order) o;
        return order_isd == order.order_isd && product_id == order.product_id && order_status == order.order_status && Objects.equals(order_date, order.order_date) && Objects.equals(customer_name, order.customer_name) && Objects.equals(price, order.price);
    }

    @Override
    public int hashCode() {
        return Objects.hash(order_isd, order_date, customer_name, price, product_id, order_status);
    }
}
