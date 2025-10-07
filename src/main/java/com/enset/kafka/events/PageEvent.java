package com.enset.kafka.events;

import java.util.Date;

public record PageEvent(String name, String user, Date date, long duration) {
}
