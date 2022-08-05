package com.anabneri.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
@Entity
public class Book {

    @Id
    private Integer bookId;
    private String bookName;
    private String bookAuthor;

    @OneToOne
    @JoinColumn(name = "bookStoreEventId")
    private BookStoreEvent bookStoreEvent;

}
