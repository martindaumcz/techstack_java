package org.mdaum.techstack.rest;

import org.mdaum.techstack.sqs.model.SqsSampleMessage;
import org.mdaum.techstack.sqs.service.SqsMessageServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("sqs")
public class SqsController {

    private final SqsMessageServiceImpl sqsMessageService;

    @Autowired
    public SqsController(SqsMessageServiceImpl sqsMessageService) {
        this.sqsMessageService = sqsMessageService;
    }

    @PostMapping
    public void sendMessage(@RequestBody SqsSampleMessage sqsMessage) {
        sqsMessageService.sendMessage(sqsMessage);
    }

    @GetMapping
    @ResponseBody
    public List<SqsSampleMessage> receiveMessages() {
        return sqsMessageService.receiveMessages();
    }
}
