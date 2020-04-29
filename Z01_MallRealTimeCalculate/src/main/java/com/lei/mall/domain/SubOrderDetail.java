package com.lei.mall.domain;

import java.io.Serializable;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-04-29 10:14
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class SubOrderDetail implements Serializable {
    private static final long serialVersionUID = 1L;

    private long userId;
    private long orderId;
    private long subOrderId;
    private long siteId;
    private String siteName;
    private long cityId;
    private String cityName;
    private long warehouseId;
    private long merchandiseId;
    private long price;
    private long quantity;
    private int orderStatus;
    private int isNewOrder;
    private long timestamp;
}
