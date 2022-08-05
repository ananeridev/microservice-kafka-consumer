package com.anabneri.jpa;

import com.anabneri.entity.BookStoreEvent;
import org.springframework.data.repository.CrudRepository;

public interface BookStoreEventRepository extends CrudRepository<BookStoreEvent, Integer> {
}
