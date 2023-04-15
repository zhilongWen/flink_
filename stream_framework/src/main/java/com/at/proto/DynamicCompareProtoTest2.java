package com.at.proto;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import java.io.FileInputStream;
import java.util.Arrays;
import java.util.HashMap;

/**
 * @author zero
 * @create 2023-04-15
 */
public class DynamicCompareProtoTest2 {

    public static void main(String[] args) throws Exception{

        UserMessageProto.UserMessage userMessage = init();

        //product
        System.out.println("--------------protuct--------------");
        DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos
                .FileDescriptorSet
                .parseFrom(new FileInputStream("D:\\workspace\\flink_\\stream_framework\\src\\main\\java\\com\\at\\proto\\UserMessage.desc"));


        System.out.println("========== descriptorSet ==========");
        System.out.println(descriptorSet);
        System.out.println("========== getFullName ==========");
        System.out.println(userMessage.getDescriptorForType().getFullName());
        System.out.println("========== toByteString ==========");
        System.out.println(userMessage.toByteString());

        SelfDescribingMessageProto.SelfDescribingMessage.Builder newBuilder = SelfDescribingMessageProto.SelfDescribingMessage.newBuilder();
        SelfDescribingMessageProto.SelfDescribingMessage selfDescribingMessage = newBuilder
                .setDescriptorSet(descriptorSet)
                .setMsgName(userMessage.getDescriptorForType().getFullName())
                .setMessage(userMessage.toByteString())
                .build();
        System.out.println(selfDescribingMessage);
        byte[] byteArray = selfDescribingMessage.toByteArray();
        System.out.println(byteArray);


        //customer 无需任何外部的 proto 类文件或描述符文件，只需统一的  SelfDescribingMessage java 文件
        System.out.println("--------------customer--------------");

        SelfDescribingMessageProto.SelfDescribingMessage parseFrom = SelfDescribingMessageProto
                .SelfDescribingMessage
                .parseFrom(byteArray);
        DescriptorProtos.FileDescriptorSet fromDescriptorSet = parseFrom.getDescriptorSet();
        ByteString message = parseFrom.getMessage();
        String msgName = parseFrom.getMsgName();

        System.out.println("=========== consumer fromDescriptorSet ===========");
        System.out.println(fromDescriptorSet);
        System.out.println("=========== consumer message ===========");
        System.out.println(message);
        System.out.println("=========== consumer msgName ===========");
        System.out.println(msgName);

        Descriptors.Descriptor pbDescritpor = null;
        for (DescriptorProtos.FileDescriptorProto fpd : fromDescriptorSet.getFileList()) {

            System.out.println(fpd);

            Descriptors.FileDescriptor fileDescriptor = Descriptors
                    .FileDescriptor
                    .buildFrom(fpd, new Descriptors.FileDescriptor[]{});

            System.out.println(fileDescriptor);

            for (Descriptors.Descriptor messageType : fileDescriptor.getMessageTypes()) {
                System.out.println(messageType);
                if (msgName.equals(messageType.getName())){
                    pbDescritpor = messageType;
                    break;
                }
            }
        }

        if (pbDescritpor == null) {
            System.out.println("No matched descriptor");
            return;
        }
        DynamicMessage dmsg = DynamicMessage.parseFrom(pbDescritpor, message);

        System.out.println(dmsg);


    }

    public static UserMessageProto.UserMessage init() {
        UserMessageProto.Hobby hobby1 = UserMessageProto.Hobby.newBuilder()
                .setKeyFlag("play_mud")
                .setName("完泥巴")
                .addAllHobbyAddr(Arrays.asList("mud_1", "mud_2"))
                .build();

        UserMessageProto.Hobby hobby2 = UserMessageProto.Hobby.newBuilder()
                .setKeyFlag("play_badminton")
                .setName("打羽毛球")
                .addAllHobbyAddr(Arrays.asList("badminton_1"))
                .build();

        UserMessageProto.GoodsInfo goodsInfo1 = UserMessageProto.GoodsInfo
                .newBuilder()
                .setGoodsId(3609832638461459L)
                .setBrandId(10665)
                .setCat1Id(103)
                .setCat3Id(20378)
                .setCat3Id(32987)
                .setSpuId(10936748384590740L)
                .build();

        UserMessageProto.UserMessage userMessage = UserMessageProto
                .UserMessage
                .newBuilder()
                .setUpdateTime(System.currentTimeMillis())
                .setName("老六")
                .setGenderValue(UserMessageProto.Gender.MAN_VALUE)
                .addAllClkGoodSeq(Arrays.asList(goodsInfo1))
                .putAllHobbyInfo(
                        new HashMap<String, UserMessageProto.Hobby>() {{
                            put("play_mud", hobby1);
                            put("play_badminton", hobby2);
                        }}
                )
                .build();

        System.out.println(userMessage);

        return userMessage;
    }

}
