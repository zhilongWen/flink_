package com.at;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.Mappings;
import org.mapstruct.factory.Mappers;

import java.util.List;

/**
 * @create 2022-09-19
 */
@Mapper
public interface ProductMapper {

    /*
        单个
            字段相同key不写 @Mappings
            字段不同要写 @Mappings 否则字段不同的无法赋值

     */

    ProductMapper mapper = Mappers.getMapper(ProductMapper.class);

    /**
     * 将product转换成productvo
     *
     * @param product
     * @return
     */
    @Mappings(
            {
                    @Mapping(source = "id", target = "id"),
                    @Mapping(source = "name", target = "name"),
                    @Mapping(source = "price", target = "price")
            }
    )
    ProductVO productVO(Product product);

    /**
     * 集合转换成集合操作
     * @param productList
     * @return
     */
    List<ProductVO> productVOList(List<Product> productList);

    /**
     * 将product转换成productvo2
     *
     * @param product
     * @return
     */
//    @Mappings(
//            {
//                    @Mapping(source = "id", target = "pid"),
//                    @Mapping(source = "name", target = "pname"),
//                    @Mapping(source = "price", target = "price")
//            }
//    )
//    ProductVO2 productVO2(Product product);

    /**
     * 集合转换成集合操作
     * @param productList
     * @return
     */
    List<ProductVO2> productVO2List(List<Product> productList);



}
