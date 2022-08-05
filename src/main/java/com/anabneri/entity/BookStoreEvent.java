package com.anabneri.entity;

import lombok.*;

import javax.persistence.*;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;


@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
@Entity
public class BookStoreEvent {

    @Id
    @GeneratedValue
    private Integer bookStoreEventId;

    @Enumerated(EnumType.STRING)
    private BookStoreEventType bookStoreEventType;

    @OneToOne(mappedBy = "bookStoreEvent", cascade = {CascadeType.ALL})
    @ToString.Exclude
    private Book book;

}
