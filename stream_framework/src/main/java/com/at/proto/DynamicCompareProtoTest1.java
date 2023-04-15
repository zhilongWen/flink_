package com.at.proto;

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
public class DynamicCompareProtoTest1 {
    public static void main(String[] args) throws Exception {


        System.out.println("init message -----------------------");
        byte[] bytes = init();

        System.out.println("------generate descriptor------");

        // E:\devsoft\protoc-3.15.3-win64\bin\protoc.exe --descriptor_set_out=D:\workspace\flink_\stream_framework\src\main\java\com\at\proto\UserMessage.desc --proto_path=D:\workspace\flink_\stream_framework\src\main\java\com\at\proto\ UserMessage.proto

        String cmd = "E:\\devsoft\\protoc-3.15.3-win64\\bin\\protoc.exe --descriptor_set_out=D:\\workspace\\flink_\\stream_framework\\src\\main\\java\\com\\at\\proto\\UserMessage.desc --proto_path=D:\\workspace\\flink_\\stream_framework\\src\\main\\java\\com\\at\\proto\\ UserMessage.proto";

        Process process = Runtime.getRuntime().exec(cmd);
        process.waitFor();

        int exitValue = process.exitValue();

        if (exitValue != 0) {
            System.out.println("protoc execute failed");
            return;
        }


        // customer 阶段无需 proto 文件生成的 java lei，但是需要 proto 文件生成的文件描述符
        System.out.println("------customer msg------");

        Descriptors.Descriptor pbDescritpor = null;

        DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos
                .FileDescriptorSet
                .parseFrom(new FileInputStream("D:\\workspace\\flink_\\stream_framework\\src\\main\\java\\com\\at\\proto\\UserMessage.desc"));

        for (DescriptorProtos.FileDescriptorProto fpd : descriptorSet.getFileList()) {

            System.out.println(fpd);

            Descriptors.FileDescriptor fileDescriptor = Descriptors.FileDescriptor.buildFrom(fpd, new Descriptors.FileDescriptor[]{});

            System.out.println(fileDescriptor);

            for (Descriptors.Descriptor descriptor : fileDescriptor.getMessageTypes()) {

                System.out.println(descriptor);

                String className = fpd.getOptions().getJavaPackage() + "." + fpd.getOptions().getJavaOuterClassname() + "$" + descriptor.getName();

                System.out.println(className);
                System.out.println(descriptor.getFullName() + " -> " + className);

                if ("UserMessage".equals(descriptor.getName())) {
                    pbDescritpor = descriptor;
                    break;
                }

            }

            if (pbDescritpor == null) {
                System.out.println("No matched descriptor");
                return;
            }

            System.out.println(pbDescritpor.getName());

            DynamicMessage.Builder pbBuilder = DynamicMessage.newBuilder(pbDescritpor);

            DynamicMessage pbMessage = pbBuilder.mergeFrom(bytes).build();

            System.out.println(pbMessage);

            System.out.println("==============================");
            DynamicMessage dynamicMessage = DynamicMessage.parseFrom(pbDescritpor, bytes);
            System.out.println(dynamicMessage);

            System.out.println("==============================");

        }
    }

    public static byte[] init() {
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

        return userMessage.toByteArray();
    }


}
